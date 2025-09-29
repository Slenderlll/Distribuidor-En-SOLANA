from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional, Sequence, TypeVar

import base58
from solders.hash import Hash
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.signature import Signature
from solders.system_program import TransferParams, transfer
from solders.transaction import Transaction
from solana.rpc.api import Client
from solana.rpc.commitment import Commitment, Confirmed
from solana.rpc.core import RPCException
from solana.rpc.types import TxOpts
from solders.compute_budget import set_compute_unit_limit, set_compute_unit_price

T = TypeVar("T")

LAMPORTS_PER_SOL = 1_000_000_000
DEVNET_RPC_URL = "https://api.devnet.solana.com"
TESTNET_RPC_URL = "https://api.testnet.solana.com"
MAINNET_RPC_URL = "https://api.mainnet-beta.solana.com"
ANKR_MAINNET_RPC = "https://rpc.ankr.com/solana"
ANKR_DEVNET_RPC = "https://rpc.ankr.com/solana_devnet"
ANKR_TESTNET_RPC = "https://rpc.ankr.com/solana_testnet"


class WalletLoadError(RuntimeError):
    """Raised when the source wallet cannot be loaded."""


class RecipientParseError(ValueError):
    """Raised when the recipients list cannot be parsed."""


class WalletNotLoadedError(RuntimeError):
    """Raised when an operation that needs a wallet is attempted without one."""


@dataclass
class Recipient:
    address: str
    lamports: int


class SolanaPayoutManager:
    """Utility class that helps fund wallets on Solana devnet/testnet/mainnet."""

    def __init__(
        self,
        endpoint: str = DEVNET_RPC_URL,
        timeout: float = 10.0,
        max_retries_per_endpoint: int = 3,
        retry_backoff_seconds: float = 0.5,
        accounts_query_chunk: int = 95,
        default_max_recipients_per_tx: int = 10,
        default_compute_unit_limit: Optional[int] = 200_000,
        default_compute_unit_price_micro_lamports: Optional[int] = None,
    ) -> None:
        self.timeout = timeout
        self.max_retries_per_endpoint = max(1, max_retries_per_endpoint)
        self.retry_backoff_seconds = max(0.0, retry_backoff_seconds)
        self.accounts_query_chunk = max(1, accounts_query_chunk)
        self.default_max_recipients_per_tx = max(1, default_max_recipients_per_tx)
        self.default_compute_unit_limit = default_compute_unit_limit
        self.default_compute_unit_price = default_compute_unit_price_micro_lamports
        self.rpc_endpoints: List[str] = []
        self._current_endpoint_index = 0
        self.client = Client(endpoint, timeout=timeout)
        self.last_recipient_warnings: List[str] = []
        self.set_rpc_pool([endpoint])

    # ------------------------------------------------------------------
    # Pool management & RPC helpers
    # ------------------------------------------------------------------
    def set_endpoint(self, endpoint: str, timeout: Optional[float] = None) -> None:
        """Switch to a single RPC endpoint (resets the pool)."""
        self.set_rpc_pool([endpoint], max_retries=None, retry_backoff=None, timeout=timeout)

    def set_rpc_pool(
        self,
        endpoints: Sequence[str],
        max_retries: Optional[int] = None,
        retry_backoff: Optional[float] = None,
        timeout: Optional[float] = None,
    ) -> None:
        unique: List[str] = []
        seen: set[str] = set()
        for endpoint in endpoints:
            cleaned = endpoint.strip()
            if not cleaned:
                continue
            if cleaned not in seen:
                unique.append(cleaned)
                seen.add(cleaned)
        if not unique:
            raise ValueError("Se necesita al menos un endpoint RPC válido")
        if max_retries is not None:
            if max_retries < 1:
                raise ValueError("max_retries debe ser al menos 1")
            self.max_retries_per_endpoint = max_retries
        if retry_backoff is not None:
            if retry_backoff < 0:
                raise ValueError("retry_backoff no puede ser negativo")
            self.retry_backoff_seconds = retry_backoff
        if timeout is not None:
            self.timeout = timeout
        self.rpc_endpoints = unique
        self._current_endpoint_index = 0
        self.endpoint = self.rpc_endpoints[0]
        self.client = Client(self.endpoint, timeout=self.timeout)

    def _perform(self, func: Callable[[Client], T]) -> T:
        attempts = 0
        total_endpoints = len(self.rpc_endpoints)
        max_attempts = total_endpoints * self.max_retries_per_endpoint
        last_error: Optional[Exception] = None
        while attempts < max_attempts:
            try:
                return func(self.client)
            except Exception as exc:  # pragma: no cover - network dependent
                last_error = exc
                attempts += 1
                time.sleep(self.retry_backoff_seconds * min(attempts, 4))
                if total_endpoints > 1 and attempts % self.max_retries_per_endpoint == 0:
                    self._rotate_endpoint()
        raise RuntimeError(
            f"Error tras {max_attempts} intentos usando pool RPC {self.rpc_endpoints}: {last_error}"
        ) from last_error

    def _rotate_endpoint(self) -> None:
        if len(self.rpc_endpoints) <= 1:
            return
        self._current_endpoint_index = (self._current_endpoint_index + 1) % len(self.rpc_endpoints)
        self.endpoint = self.rpc_endpoints[self._current_endpoint_index]
        self.client = Client(self.endpoint, timeout=self.timeout)

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    def ping(self, timeout_seconds: float = 5.0) -> float:
        start = time.perf_counter()
        is_ok = self._perform(lambda client: client.is_connected())
        elapsed = time.perf_counter() - start
        if not is_ok:
            raise RuntimeError("El RPC respondió pero no está saludable")
        if elapsed > timeout_seconds:
            raise TimeoutError(
                f"El RPC respondió en {elapsed:.2f}s, excediendo el límite de {timeout_seconds}s"
            )
        return elapsed

    def load_wallet_from_file(self, path: str | Path) -> str:
        file_path = Path(path).expanduser().resolve()
        if not file_path.exists():
            raise WalletLoadError(f"No se encontró el archivo de la wallet: {file_path}")
        raw = file_path.read_text(encoding="utf-8").strip()
        if not raw:
            raise WalletLoadError(f"El archivo {file_path} está vacío")
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            data = None
        if isinstance(data, list):
            secret_bytes = bytes(int(value) for value in data)
            self.wallet = self._keypair_from_bytes(secret_bytes)
            return str(self.wallet.pubkey())
        try:
            secret_bytes = base58.b58decode(raw)
        except ValueError as exc:
            raise WalletLoadError(f"No se pudo interpretar la clave secreta en {file_path}") from exc
        self.wallet = self._keypair_from_bytes(secret_bytes)
        return str(self.wallet.pubkey())

    def get_wallet_address(self) -> str:
        wallet = self._require_wallet()
        return str(wallet.pubkey())

    def get_balance_sol(self, commitment: Commitment = Confirmed) -> Decimal:
        wallet = self._require_wallet()
        response = self._perform(lambda client: client.get_balance(wallet.pubkey(), commitment=commitment))
        lamports = response.value
        return Decimal(lamports) / Decimal(LAMPORTS_PER_SOL)

    def request_airdrop(
        self,
        amount_sol: float | Decimal,
        commitment: Commitment = Confirmed,
        max_per_request_sol: float = 2.0,
        max_attempts: int = 3,
        pause_seconds: float = 2.0,
    ) -> List[str]:
        wallet = self._require_wallet()
        decimal_amount = amount_sol if isinstance(amount_sol, Decimal) else Decimal(str(amount_sol))
        if decimal_amount <= 0:
            raise ValueError("La cantidad debe ser mayor que cero")
        chunk_limit = Decimal(str(max_per_request_sol))
        if chunk_limit <= 0:
            raise ValueError("max_per_request_sol debe ser mayor que cero")
        remaining = decimal_amount
        signatures: List[str] = []
        while remaining > Decimal("0"):
            chunk_amount = remaining if remaining <= chunk_limit else chunk_limit
            lamports = self._sol_to_lamports(chunk_amount)
            signature = self._request_airdrop_lamports(
                wallet.pubkey(),
                lamports,
                commitment=commitment,
                max_attempts=max_attempts,
                pause_seconds=pause_seconds,
            )
            signatures.append(signature)
            self._await_confirmation(signature, commitment=commitment)
            remaining -= chunk_amount
            if remaining > Decimal("0"):
                time.sleep(pause_seconds)
        return signatures

    def _request_airdrop_lamports(
        self,
        pubkey: Pubkey,
        lamports: int,
        commitment: Commitment = Confirmed,
        max_attempts: int = 3,
        pause_seconds: float = 2.0,
    ) -> str:
        attempt = 0
        last_error: Optional[Exception] = None
        while attempt < max_attempts:
            attempt += 1
            try:
                response = self._perform(
                    lambda client: client.request_airdrop(pubkey, lamports, commitment=commitment)
                )
            except Exception as exc:  # pragma: no cover - network dependent
                last_error = exc
                time.sleep(pause_seconds * attempt)
                continue
            signature = self._extract_signature(response)
            return signature
        raise RuntimeError(f"Error al solicitar airdrop tras {max_attempts} intentos: {last_error}")

    def read_recipients_from_file(
        self,
        path: str | Path,
        default_amount_sol: Optional[float | Decimal] = None,
    ) -> List[Recipient]:
        file_path = Path(path).expanduser().resolve()
        if not file_path.exists():
            raise RecipientParseError(f"No se encontró el archivo de wallets: {file_path}")
        raw_lines = file_path.read_text(encoding="utf-8").splitlines()
        recipients_raw: List[Recipient] = []
        warnings: List[str] = []
        for line_number, raw_line in enumerate(raw_lines, start=1):
            line = raw_line.strip()
            line = line.lstrip("\ufeff")
            if not line or line.startswith("#"):
                continue
            try:
                recipient = self._parse_recipient_line(line, line_number, default_amount_sol)
            except RecipientParseError as exc:
                warnings.append(str(exc))
                continue
            recipients_raw.append(recipient)
        aggregated: Dict[str, Recipient] = {}
        duplicates_counter: Dict[str, int] = {}
        for recipient in recipients_raw:
            if recipient.address in aggregated:
                aggregated_rec = aggregated[recipient.address]
                aggregated_rec.lamports += recipient.lamports
                duplicates_counter[recipient.address] = duplicates_counter.get(recipient.address, 0) + 1
            else:
                aggregated[recipient.address] = Recipient(recipient.address, recipient.lamports)
        for address, count in duplicates_counter.items():
            warnings.append(
                f"La dirección {address} aparece {count + 1} veces; los montos se sumaron automáticamente."
            )
        recipients = list(aggregated.values())
        self.last_recipient_warnings = warnings
        if not recipients:
            if warnings:
                raise RecipientParseError("No se pudieron cargar destinatarios válidos. Revisa el archivo.")
            raise RecipientParseError(
                "El archivo de wallets está vacío o todas las líneas son comentarios."
            )
        return recipients

    def send_mass_payments(
        self,
        recipients: Sequence[Recipient],
        max_per_transaction: Optional[int] = None,
        skip_preflight: bool = False,
        commitment: Commitment = Confirmed,
        compute_unit_limit: Optional[int] = None,
        compute_unit_price_micro_lamports: Optional[int] = None,
    ) -> List[str]:
        batch_size = max_per_transaction or self.default_max_recipients_per_tx
        if batch_size < 1:
            raise ValueError("max_per_transaction debe ser al menos 1")
        wallet = self._require_wallet()
        signatures: List[str] = []
        for chunk in self._chunk(recipients, batch_size):
            instructions = []
            limit_value = compute_unit_limit if compute_unit_limit is not None else self.default_compute_unit_limit
            if limit_value is not None:
                instructions.append(set_compute_unit_limit(int(limit_value)))
            price_value = (
                compute_unit_price_micro_lamports
                if compute_unit_price_micro_lamports is not None
                else self.default_compute_unit_price
            )
            if price_value is not None:
                instructions.append(set_compute_unit_price(int(price_value)))
            for recipient in chunk:
                instruction = transfer(
                    TransferParams(
                        from_pubkey=wallet.pubkey(),
                        to_pubkey=Pubkey.from_string(recipient.address),
                        lamports=recipient.lamports,
                    )
                )
                instructions.append(instruction)
            blockhash_resp = self._perform(
                lambda client: client.get_latest_blockhash(commitment=commitment)
            )
            blockhash: Hash = blockhash_resp.value.blockhash
            tx = Transaction.new_signed_with_payer(
                instructions,
                wallet.pubkey(),
                [wallet],
                blockhash,
            )
            response = self._perform(
                lambda client: client.send_transaction(
                    tx,
                    opts=TxOpts(skip_preflight=skip_preflight, preflight_commitment=commitment),
                )
            )
            signature = self._extract_signature(response)
            self._await_confirmation(signature, commitment=commitment)
            signatures.append(signature)
        return signatures

    def fetch_balances(
        self,
        addresses: Sequence[str],
        commitment: Commitment = Confirmed,
    ) -> Dict[str, Decimal]:
        if not addresses:
            return {}
        order: List[str] = []
        unique: List[str] = []
        seen: set[str] = set()
        for address in addresses:
            order.append(address)
            if address not in seen:
                unique.append(address)
                seen.add(address)
        balances_unique: Dict[str, Decimal] = {}
        for chunk in self._chunk(unique, self.accounts_query_chunk):
            pubkeys = [Pubkey.from_string(address) for address in chunk]
            response = self._perform(
                lambda client: client.get_multiple_accounts(pubkeys, commitment=commitment)
            )
            for address, account in zip(chunk, response.value):
                if account is None:
                    balances_unique[address] = Decimal("0")
                else:
                    balances_unique[address] = Decimal(account.lamports) / Decimal(LAMPORTS_PER_SOL)
        return {address: balances_unique.get(address, Decimal("0")) for address in order}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _parse_recipient_line(
        self,
        line: str,
        line_number: int,
        default_amount_sol: Optional[float | Decimal],
    ) -> Recipient:
        tokens = [token for token in re.split(r"[\s,;]+", line) if token]
        if not tokens:
            raise RecipientParseError(f"La línea {line_number} está vacía")
        address = tokens[0]
        try:
            Pubkey.from_string(address)
        except Exception as exc:
            raise RecipientParseError(
                f"La dirección en la línea {line_number} es inválida: {address}"
            ) from exc
        if len(tokens) > 1:
            amount_text = tokens[1]
            try:
                amount_sol = Decimal(amount_text)
            except Exception as exc:
                raise RecipientParseError(
                    f"Cantidad inválida en la línea {line_number}: {amount_text}"
                ) from exc
        else:
            if default_amount_sol is None:
                raise RecipientParseError(
                    f"La línea {line_number} no incluye cantidad y no se proporcionó un valor por defecto."
                )
            amount_sol = (
                default_amount_sol
                if isinstance(default_amount_sol, Decimal)
                else Decimal(str(default_amount_sol))
            )
        lamports = self._sol_to_lamports(amount_sol)
        return Recipient(address=address, lamports=lamports)

    def _require_wallet(self) -> Keypair:
        if not hasattr(self, "wallet") or self.wallet is None:
            raise WalletNotLoadedError("Carga primero la wallet madre antes de operar.")
        return self.wallet

    def _keypair_from_bytes(self, secret_bytes: bytes) -> Keypair:
        if len(secret_bytes) == 64:
            return Keypair.from_bytes(secret_bytes)
        if len(secret_bytes) == 32:
            return Keypair.from_seed(secret_bytes)
        raise WalletLoadError(
            "La clave secreta debe tener 32 bytes (seed) o 64 bytes (full secret key)."
        )

    @staticmethod
    def _sol_to_lamports(amount_sol: Decimal | float | str) -> int:
        decimal_amount = amount_sol if isinstance(amount_sol, Decimal) else Decimal(str(amount_sol))
        if decimal_amount <= 0:
            raise ValueError("La cantidad debe ser mayor que cero")
        lamports = int((decimal_amount * Decimal(LAMPORTS_PER_SOL)).to_integral_value(rounding=ROUND_DOWN))
        if lamports <= 0:
            raise ValueError("La cantidad es demasiado pequeña (0 lamports)")
        return lamports

    def _await_confirmation(
        self,
        signature: str,
        timeout_seconds: float = 60.0,
        commitment: Commitment = Confirmed,
    ) -> None:
        deadline = time.time() + timeout_seconds
        last_status: Optional[object] = None
        signature_obj = Signature.from_string(signature)
        while time.time() < deadline:
            response = self._perform(lambda client: client.get_signature_statuses([signature_obj]))
            status = response.value[0]
            if status is None:
                time.sleep(0.8)
                continue
            if status.err is not None:
                raise RuntimeError(f"La transacción {signature} falló: {status.err}")
            confirmation_status = status.confirmation_status
            if confirmation_status is not None and str(confirmation_status).lower() in {"confirmed", "finalized"}:
                return
            confirmations = status.confirmations
            if confirmations is not None and confirmations > 0:
                return
            last_status = status
            time.sleep(0.8)
        raise TimeoutError(
            f"No se confirmó la transacción {signature} dentro de {timeout_seconds} segundos. Último estado: {last_status}"
        )

    @staticmethod
    def _extract_signature(response) -> str:
        if isinstance(response, Signature):
            return str(response)
        value = getattr(response, "value", None)
        if isinstance(value, Signature):
            return str(value)
        if isinstance(value, str):
            return value
        raise RuntimeError(f"No se pudo extraer la firma de la respuesta: {response}")

    def _chunk(self, items: Sequence[T], size: int) -> Iterator[Sequence[T]]:
        for index in range(0, len(items), size):
            yield items[index : index + size]

    def sum_lamports(self, recipients: Sequence[Recipient]) -> int:
        return sum(recipient.lamports for recipient in recipients)
