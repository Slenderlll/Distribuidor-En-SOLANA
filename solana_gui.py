from __future__ import annotations

import threading
import time
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from solana_manager import (
    ANKR_DEVNET_RPC,
    ANKR_MAINNET_RPC,
    ANKR_TESTNET_RPC,
    DEVNET_RPC_URL,
    LAMPORTS_PER_SOL,
    MAINNET_RPC_URL,
    Recipient,
    RecipientParseError,
    SolanaPayoutManager,
    TESTNET_RPC_URL,
    WalletLoadError,
    WalletNotLoadedError,
)

NETWORK_CHOICES: Dict[str, str] = {
    "Devnet (Solana)": DEVNET_RPC_URL,
    "Devnet (Ankr)": ANKR_DEVNET_RPC,
    "Testnet (Solana)": TESTNET_RPC_URL,
    "Testnet (Ankr)": ANKR_TESTNET_RPC,
    "Mainnet-Beta (Solana)": MAINNET_RPC_URL,
    "Mainnet (Ankr)": ANKR_MAINNET_RPC,
    "Personalizado": "",
}

MAX_LOG_LINES = 500


class SolanaDistributorGUI(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Solana Devnet Distributor")
        self.geometry("1040x780")
        self.minsize(940, 700)

        self.manager = SolanaPayoutManager()
        self.recipients: List[Recipient] = []
        self.recipient_balances: Dict[str, Decimal] = {}

        self.network_var = tk.StringVar(value="Devnet (Solana)")
        self.rpc_url_var = tk.StringVar(value=DEVNET_RPC_URL)
        self.wallet_address_var = tk.StringVar(value="Wallet no cargada")
        self.balance_var = tk.StringVar(value="0.0000 SOL")
        self.airdrop_amount_var = tk.StringVar(value="1")
        self.default_amount_var = tk.StringVar(value="0.1")
        self.recipient_file_var = tk.StringVar(value="")
        self.recipient_count_var = tk.StringVar(value="0 direcciones")
        self.total_sol_var = tk.StringVar(value="0.0000 SOL")
        self.ping_var = tk.StringVar(value="Ping no realizado")

        self.max_per_tx_var = tk.StringVar(value=str(self.manager.default_max_recipients_per_tx))
        self.compute_unit_limit_var = tk.StringVar(
            value="" if self.manager.default_compute_unit_limit is None else str(self.manager.default_compute_unit_limit)
        )
        self.priority_fee_var = tk.StringVar(
            value="" if self.manager.default_compute_unit_price is None else str(self.manager.default_compute_unit_price)
        )

        self._default_reload_job: Optional[str] = None

        self.action_buttons: List[ttk.Button] = []
        self.style = ttk.Style(self)
        self._configure_theme()
        self._build_ui()
        self._apply_endpoint()

        self.default_amount_var.trace_add("write", self._on_default_amount_change)

    # ------------------------------------------------------------------
    # UI construction & styling
    # ------------------------------------------------------------------
    def _configure_theme(self) -> None:
        available = set(self.style.theme_names())
        if "clam" in available:
            self.style.theme_use("clam")
        self.style.configure("TLabel", padding=(2, 2))
        self.style.configure("Section.TLabelframe", padding=(12, 10))
        self.style.configure("Section.TLabelframe.Label", font=("Segoe UI", 10, "bold"))
        self.style.configure("Accent.TButton", font=("Segoe UI", 10, "bold"))
        self.style.map("Accent.TButton", foreground=[("disabled", "#888")])
        self.style.configure("Recipient.Treeview", rowheight=26, font=("Segoe UI", 10))
        self.style.configure("Recipient.Treeview.Heading", font=("Segoe UI", 10, "bold"))

    def _build_ui(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        notebook = ttk.Notebook(self)
        notebook.grid(row=0, column=0, sticky="nsew", padx=12, pady=12)

        control_page = ttk.Frame(notebook)
        recipients_page = ttk.Frame(notebook)
        notebook.add(control_page, text="Panel de control")
        notebook.add(recipients_page, text="Destinatarios")

        # Panel de control -------------------------------------------------
        control_page.columnconfigure(0, weight=1)
        control_page.rowconfigure(3, weight=1)

        network_frame = ttk.LabelFrame(control_page, text="Red y endpoint", style="Section.TLabelframe")
        network_frame.grid(row=0, column=0, padx=8, pady=(8, 4), sticky="nsew")
        for col in range(5):
            network_frame.columnconfigure(col, weight=1 if col == 1 else 0)

        ttk.Label(network_frame, text="Red:").grid(row=0, column=0, sticky="w", padx=8, pady=6)
        network_combo = ttk.Combobox(
            network_frame,
            textvariable=self.network_var,
            values=list(NETWORK_CHOICES.keys()),
            state="readonly",
            width=24,
        )
        network_combo.grid(row=0, column=1, sticky="ew", padx=8, pady=6)
        network_combo.bind("<<ComboboxSelected>>", self._on_network_change)

        ttk.Label(network_frame, text="RPC URL:").grid(row=1, column=0, sticky="w", padx=8, pady=6)
        self.rpc_url_entry = ttk.Entry(network_frame, textvariable=self.rpc_url_var)
        self.rpc_url_entry.grid(row=1, column=1, sticky="ew", padx=8, pady=6)

        connect_btn = ttk.Button(network_frame, text="Conectar", command=self._apply_endpoint)
        connect_btn.grid(row=0, column=2, rowspan=2, padx=8, pady=6)
        self.action_buttons.append(connect_btn)

        pool_btn = ttk.Button(network_frame, text="Configurar pool...", command=self._open_rpc_pool_dialog)
        pool_btn.grid(row=0, column=3, rowspan=2, padx=8, pady=6)
        self.action_buttons.append(pool_btn)

        ping_btn = ttk.Button(network_frame, text="Ping RPC", command=self._ping_endpoint)
        ping_btn.grid(row=0, column=4, rowspan=2, padx=8, pady=6)
        self.action_buttons.append(ping_btn)

        ttk.Label(network_frame, textvariable=self.ping_var).grid(row=2, column=0, columnspan=5, sticky="w", padx=8, pady=(0, 6))

        wallet_frame = ttk.LabelFrame(control_page, text="Wallet madre", style="Section.TLabelframe")
        wallet_frame.grid(row=1, column=0, padx=8, pady=4, sticky="nsew")
        wallet_frame.columnconfigure(1, weight=1)

        ttk.Label(wallet_frame, text="Dirección:").grid(row=0, column=0, sticky="nw", padx=8, pady=6)
        wallet_label = ttk.Label(wallet_frame, textvariable=self.wallet_address_var, font=("Consolas", 10))
        wallet_label.grid(row=0, column=1, sticky="w", padx=8, pady=6)

        ttk.Label(wallet_frame, text="Balance:").grid(row=1, column=0, sticky="w", padx=8, pady=6)
        balance_label = ttk.Label(wallet_frame, textvariable=self.balance_var)
        balance_label.grid(row=1, column=1, sticky="w", padx=8, pady=6)

        load_wallet_btn = ttk.Button(wallet_frame, text="Cargar wallet...", command=self._select_wallet_file)
        load_wallet_btn.grid(row=0, column=2, rowspan=2, sticky="ns", padx=8, pady=6)
        self.action_buttons.append(load_wallet_btn)

        airdrop_frame = ttk.LabelFrame(control_page, text="Solicitar airdrop (solo devnet/testnet)", style="Section.TLabelframe")
        airdrop_frame.grid(row=2, column=0, padx=8, pady=4, sticky="nsew")
        airdrop_frame.columnconfigure(1, weight=1)

        ttk.Label(airdrop_frame, text="Cantidad SOL:").grid(row=0, column=0, sticky="w", padx=8, pady=6)
        amount_entry = ttk.Entry(airdrop_frame, textvariable=self.airdrop_amount_var)
        amount_entry.grid(row=0, column=1, sticky="ew", padx=8, pady=6)

        request_btn = ttk.Button(airdrop_frame, text="Solicitar airdrop", style="Accent.TButton", command=self._request_airdrop)
        request_btn.grid(row=0, column=2, padx=8, pady=6)
        self.action_buttons.append(request_btn)

        log_frame = ttk.LabelFrame(control_page, text="Actividad", style="Section.TLabelframe")
        log_frame.grid(row=3, column=0, padx=8, pady=(4, 8), sticky="nsew")
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.log_text = tk.Text(log_frame, height=10, state="disabled", wrap="word", font=("Segoe UI", 10))
        self.log_text.grid(row=0, column=0, sticky="nsew", padx=(0, 4))
        scrollbar = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=scrollbar.set)

        # Página de destinatarios -----------------------------------------
        recipients_page.columnconfigure(0, weight=1)
        recipients_page.rowconfigure(3, weight=1)

        file_frame = ttk.Frame(recipients_page)
        file_frame.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 4))
        file_frame.columnconfigure(1, weight=1)

        ttk.Label(file_frame, text="Archivo:").grid(row=0, column=0, sticky="w", padx=4)
        recipients_entry = ttk.Entry(file_frame, textvariable=self.recipient_file_var)
        recipients_entry.grid(row=0, column=1, sticky="ew", padx=4)
        load_recipients_btn = ttk.Button(file_frame, text="Seleccionar archivo...", command=self._select_recipient_file)
        load_recipients_btn.grid(row=0, column=2, padx=4)
        self.action_buttons.append(load_recipients_btn)

        ttk.Label(file_frame, text="Monto por defecto (SOL):").grid(row=1, column=0, sticky="w", padx=4, pady=(6, 0))
        default_entry = ttk.Entry(file_frame, textvariable=self.default_amount_var)
        default_entry.grid(row=1, column=1, sticky="ew", padx=4, pady=(6, 0))
        default_entry.bind("<Return>", lambda _event: self._reload_recipients_with_default())
        default_entry.bind("<FocusOut>", lambda _event: self._reload_recipients_with_default())

        stats_frame = ttk.Frame(recipients_page)
        stats_frame.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 6))
        stats_frame.columnconfigure(1, weight=1)
        ttk.Label(stats_frame, text="Destinatarios:").grid(row=0, column=0, sticky="w")
        ttk.Label(stats_frame, textvariable=self.recipient_count_var).grid(row=0, column=1, sticky="w")
        ttk.Label(stats_frame, text="Total a enviar:").grid(row=1, column=0, sticky="w", pady=2)
        ttk.Label(stats_frame, textvariable=self.total_sol_var).grid(row=1, column=1, sticky="w", pady=2)

        tuning_frame = ttk.LabelFrame(recipients_page, text="Optimización de transacción", style="Section.TLabelframe")
        tuning_frame.grid(row=2, column=0, sticky="ew", padx=8, pady=(0, 6))
        for col in range(3):
            tuning_frame.columnconfigure(col, weight=1 if col % 2 == 1 else 0)

        ttk.Label(tuning_frame, text="Máx. wallets por transacción:").grid(row=0, column=0, sticky="w", padx=8, pady=4)
        ttk.Entry(tuning_frame, textvariable=self.max_per_tx_var).grid(row=0, column=1, sticky="ew", padx=8, pady=4)

        ttk.Label(tuning_frame, text="Compute units límite:").grid(row=1, column=0, sticky="w", padx=8, pady=4)
        ttk.Entry(tuning_frame, textvariable=self.compute_unit_limit_var).grid(row=1, column=1, sticky="ew", padx=8, pady=4)

        ttk.Label(tuning_frame, text="Priority fee (micro-lamports):").grid(row=2, column=0, sticky="w", padx=8, pady=4)
        ttk.Entry(tuning_frame, textvariable=self.priority_fee_var).grid(row=2, column=1, sticky="ew", padx=8, pady=4)

        table_frame = ttk.Frame(recipients_page, padding=(0, 4, 0, 4))
        table_frame.grid(row=3, column=0, sticky="nsew", padx=8)
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)

        self.recipient_tree = ttk.Treeview(
            table_frame,
            columns=("address", "amount", "balance"),
            show="headings",
            selectmode="extended",
            height=12,
            style="Recipient.Treeview",
        )
        self.recipient_tree.heading("address", text="Wallet")
        self.recipient_tree.heading("amount", text="Monto SOL")
        self.recipient_tree.heading("balance", text="Balance actual")
        self.recipient_tree.column("address", width=380, anchor="w")
        self.recipient_tree.column("amount", width=120, anchor="center")
        self.recipient_tree.column("balance", width=160, anchor="center")
        self.recipient_tree.grid(row=0, column=0, sticky="nsew")

        tree_scroll_y = ttk.Scrollbar(table_frame, orient="vertical", command=self.recipient_tree.yview)
        tree_scroll_y.grid(row=0, column=1, sticky="ns")
        tree_scroll_x = ttk.Scrollbar(table_frame, orient="horizontal", command=self.recipient_tree.xview)
        tree_scroll_x.grid(row=1, column=0, sticky="ew")
        self.recipient_tree.configure(yscrollcommand=tree_scroll_y.set, xscrollcommand=tree_scroll_x.set)
        self.recipient_tree.tag_configure("odd", background="white")
        self.recipient_tree.tag_configure("even", background="#f6f8ff")

        buttons_frame = ttk.Frame(recipients_page)
        buttons_frame.grid(row=4, column=0, sticky="ew", padx=8, pady=6)
        buttons_frame.columnconfigure(0, weight=1)
        buttons_frame.columnconfigure(1, weight=1)

        refresh_balances_btn = ttk.Button(buttons_frame, text="Actualizar balances", command=self._refresh_recipient_balances_async)
        refresh_balances_btn.grid(row=0, column=0, sticky="ew", padx=(0, 6))
        self.action_buttons.append(refresh_balances_btn)

        send_btn = ttk.Button(buttons_frame, text="Enviar SOL a destinatarios", style="Accent.TButton", command=self._send_to_recipients)
        send_btn.grid(row=0, column=1, sticky="ew", padx=(6, 0))
        self.action_buttons.append(send_btn)

    # ------------------------------------------------------------------
    # Network / pool actions
    # ------------------------------------------------------------------
    def _on_network_change(self, _event=None) -> None:
        choice = self.network_var.get()
        rpc_value = NETWORK_CHOICES.get(choice, "")
        self.rpc_url_var.set(rpc_value)
        self._update_rpc_entry_state(choice)
        self._apply_endpoint()

    def _update_rpc_entry_state(self, choice: str) -> None:
        if choice == "Personalizado":
            self.rpc_url_entry.configure(state="normal")
            self.rpc_url_entry.focus_set()
        else:
            self.rpc_url_entry.configure(state="readonly")

    def _apply_endpoint(self) -> None:
        url = self.rpc_url_var.get().strip()
        if not url:
            messagebox.showwarning("Endpoint", "Selecciona o ingresa un RPC válido")
            return
        try:
            self.manager.set_endpoint(url)
        except Exception as exc:
            messagebox.showerror("Endpoint", f"No se pudo configurar el RPC: {exc}")
            return
        self.ping_var.set("Ping no realizado")
        self.log(f"Conectado a {url}")
        self._refresh_balance_async()
        self._refresh_recipient_balances_async()

    def _open_rpc_pool_dialog(self) -> None:
        dialog = tk.Toplevel(self)
        dialog.title("Configurar pool RPC")
        dialog.grab_set()

        frame = ttk.Frame(dialog, padding=12)
        frame.grid(row=0, column=0, sticky="nsew")
        dialog.columnconfigure(0, weight=1)
        dialog.rowconfigure(0, weight=1)

        ttk.Label(frame, text="Endpoints (uno por línea):").grid(row=0, column=0, sticky="w")
        endpoints_text = tk.Text(frame, width=60, height=8)
        endpoints_text.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=6)
        endpoints_text.insert("1.0", "\n".join(self.manager.rpc_endpoints))

        ttk.Label(frame, text="Reintentos por endpoint:").grid(row=2, column=0, sticky="w", pady=(8, 0))
        retries_var = tk.StringVar(value=str(self.manager.max_retries_per_endpoint))
        ttk.Entry(frame, textvariable=retries_var).grid(row=2, column=1, sticky="ew", pady=(8, 0))

        ttk.Label(frame, text="Backoff (segundos):").grid(row=3, column=0, sticky="w", pady=(8, 0))
        backoff_var = tk.StringVar(value=str(self.manager.retry_backoff_seconds))
        ttk.Entry(frame, textvariable=backoff_var).grid(row=3, column=1, sticky="ew", pady=(8, 0))

        frame.columnconfigure(0, weight=1)
        frame.columnconfigure(1, weight=1)

        buttons = ttk.Frame(frame)
        buttons.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(12, 0))
        buttons.columnconfigure(0, weight=1)
        buttons.columnconfigure(1, weight=1)

        def save_pool() -> None:
            raw = endpoints_text.get("1.0", "end")
            endpoints = [line.strip() for line in raw.splitlines() if line.strip()]
            if not endpoints:
                messagebox.showerror("Pool RPC",
                                     "Ingresa al menos un endpoint válido")
                return
            try:
                retries = int(retries_var.get())
                backoff = float(backoff_var.get())
            except ValueError:
                messagebox.showerror("Pool RPC", "Valores de reintentos o backoff inválidos")
                return
            try:
                self.manager.set_rpc_pool(endpoints, max_retries=retries, retry_backoff=backoff)
            except Exception as exc:
                messagebox.showerror("Pool RPC", str(exc))
                return
            self.rpc_url_var.set(self.manager.endpoint)
            self.network_var.set("Personalizado")
            self._update_rpc_entry_state("Personalizado")
            self.log(f"Pool RPC configurado con {len(endpoints)} endpoints")
            self._refresh_balance_async()
            self._refresh_recipient_balances_async()
            dialog.destroy()

        ttk.Button(buttons, text="Guardar", command=save_pool).grid(row=0, column=0, sticky="ew", padx=(0, 6))
        ttk.Button(buttons, text="Cerrar", command=dialog.destroy).grid(row=0, column=1, sticky="ew", padx=(6, 0))

    def _ping_endpoint(self) -> None:
        def task() -> None:
            try:
                elapsed = self.manager.ping()
            except Exception as exc:
                self.log(f"Ping falló: {exc}")
                self._set_ping(f"Ping falló: {exc}")
                return
            msg = f"Ping ok: {elapsed*1000:.1f} ms"
            self.log(msg)
            self._set_ping(msg)

        self._run_in_thread(task)

    def _set_ping(self, message: str) -> None:
        self.after(0, lambda: self.ping_var.set(message))

    # ------------------------------------------------------------------
    # Wallet & airdrop actions
    # ------------------------------------------------------------------
    def _select_wallet_file(self) -> None:
        file_path = filedialog.askopenfilename(
            title="Selecciona archivo de clave",
            filetypes=[
                ("Archivos de clave", "*.json *.txt *.key"),
                ("Todos los archivos", "*.*"),
            ],
        )
        if not file_path:
            return
        try:
            address = self.manager.load_wallet_from_file(file_path)
        except WalletLoadError as exc:
            messagebox.showerror("Wallet", str(exc))
            self.log(f"Error al cargar wallet: {exc}")
            return
        self.wallet_address_var.set(address)
        self.log(f"Wallet cargada: {address}")
        self._refresh_balance_async()

    def _refresh_balance_async(self) -> None:
        def task() -> None:
            try:
                balance = self.manager.get_balance_sol()
            except WalletNotLoadedError:
                return
            except Exception as exc:
                self.log(str(exc))
                return
            self._set_balance(balance)

        self._run_background(task)

    def _set_balance(self, balance: Decimal) -> None:
        self.after(0, lambda: self.balance_var.set(f"{balance:.4f} SOL"))

    def _request_airdrop(self) -> None:
        if "Mainnet" in self.network_var.get():
            messagebox.showinfo("Airdrop", "Los airdrops solo están disponibles en devnet o testnet.")
            return
        amount_text = self.airdrop_amount_var.get().strip()
        if not amount_text:
            messagebox.showwarning("Airdrop", "Ingresa una cantidad en SOL")
            return
        try:
            amount = Decimal(amount_text)
        except Exception:
            messagebox.showerror("Airdrop", "Cantidad inválida")
            return
        self._run_in_thread(self._request_airdrop_task, amount)

    def _request_airdrop_task(self, amount: Decimal) -> None:
        try:
            signatures = self.manager.request_airdrop(amount)
        except Exception as exc:
            self.log(f"Error al solicitar airdrop: {exc}")
            self._show_error("Airdrop", str(exc))
            return
        for signature in signatures:
            self.log(f"Airdrop solicitado. Firma: {signature}")
        self._refresh_balance_async()

    # ------------------------------------------------------------------
    # Recipients management
    # ------------------------------------------------------------------
    def _select_recipient_file(self) -> None:
        file_path = filedialog.askopenfilename(
            title="Selecciona archivo de wallets",
            filetypes=[("Archivos de texto", "*.txt *.csv"), ("Todos los archivos", "*.*")],
        )
        if not file_path:
            return
        self.recipient_file_var.set(file_path)
        self._load_recipients(Path(file_path))

    def _on_default_amount_change(self, *_args) -> None:
        if not self.recipient_file_var.get().strip():
            return
        if self._default_reload_job is not None:
            self.after_cancel(self._default_reload_job)
        self._default_reload_job = self.after(500, self._reload_recipients_with_default)

    def _reload_recipients_with_default(self) -> None:
        self._default_reload_job = None
        path = self.recipient_file_var.get().strip()
        if not path:
            return
        file_path = Path(path)
        if not file_path.exists():
            return
        default_text = self.default_amount_var.get().strip()
        if not default_text:
            return
        try:
            default_amount = Decimal(default_text)
        except Exception:
            return
        try:
            self.recipients = self.manager.read_recipients_from_file(file_path, default_amount)
        except (RecipientParseError, ValueError) as exc:
            self.log(f"Error al actualizar montos: {exc}")
            return
        self.recipient_balances = {}
        self._notify_recipient_warnings()
        self._update_recipient_stats()
        self._update_recipient_table()
        self._refresh_recipient_balances_async()

    def _load_recipients(self, file_path: Path) -> None:
        default_text = self.default_amount_var.get().strip()
        default_amount = Decimal(default_text) if default_text else None
        try:
            self.recipients = self.manager.read_recipients_from_file(file_path, default_amount)
        except (RecipientParseError, ValueError) as exc:
            messagebox.showerror("Destinatarios", str(exc))
            self.log(f"Error al leer destinatarios: {exc}")
            return
        self.recipient_balances = {}
        self._notify_recipient_warnings()
        self._update_recipient_stats()
        self._update_recipient_table()
        self._refresh_recipient_balances_async()
        self.log(f"Se cargaron {len(self.recipients)} destinatarios desde {file_path}")

    def _notify_recipient_warnings(self) -> None:
        for warning in self.manager.last_recipient_warnings:
            self.log(f"Advertencia destinatario: {warning}")

    def _update_recipient_stats(self) -> None:
        count = len(self.recipients)
        total_lamports = self.manager.sum_lamports(self.recipients)
        total_sol = Decimal(total_lamports) / Decimal(LAMPORTS_PER_SOL)
        self.recipient_count_var.set(f"{count} direcciones")
        self.total_sol_var.set(f"{total_sol:.4f} SOL")

    def _update_recipient_table(self) -> None:
        existing = set(self.recipient_tree.get_children())
        desired = {recipient.address for recipient in self.recipients}
        for iid in existing - desired:
            self.recipient_tree.delete(iid)
        for index, recipient in enumerate(self.recipients):
            amount_sol = Decimal(recipient.lamports) / Decimal(LAMPORTS_PER_SOL)
            balance = self.recipient_balances.get(recipient.address)
            balance_text = f"{balance:.4f} SOL" if balance is not None else "Consultando..."
            values = (recipient.address, f"{amount_sol:.4f}", balance_text)
            if recipient.address in existing:
                self.recipient_tree.item(recipient.address, values=values)
            else:
                self.recipient_tree.insert("", "end", iid=recipient.address, values=values)
            tag = "even" if index % 2 == 0 else "odd"
            self.recipient_tree.item(recipient.address, tags=(tag,))
        visible_rows = max(6, min(len(self.recipients), 20))
        self.recipient_tree.configure(height=visible_rows)
        self.recipient_tree.tag_configure("odd", background="white")

    def _refresh_recipient_balances_async(self) -> None:
        addresses = [recipient.address for recipient in self.recipients]
        if not addresses:
            return

        def task() -> None:
            try:
                balances = self.manager.fetch_balances(addresses)
            except Exception as exc:
                self.log(f"No se pudieron obtener balances de destinatarios: {exc}")
                return
            self._set_recipient_balances(balances)

        self._run_background(task)

    def _set_recipient_balances(self, balances: Dict[str, Decimal]) -> None:
        def update() -> None:
            self.recipient_balances = balances
            for index, recipient in enumerate(self.recipients):
                amount_sol = Decimal(recipient.lamports) / Decimal(LAMPORTS_PER_SOL)
                balance = balances.get(recipient.address)
                balance_text = f"{balance:.4f} SOL" if balance is not None else "0.0000 SOL"
                tag = "even" if index % 2 == 0 else "odd"
                if self.recipient_tree.exists(recipient.address):
                    self.recipient_tree.item(
                        recipient.address,
                        values=(recipient.address, f"{amount_sol:.4f}", balance_text),
                        tags=(tag,),
                    )
        self.after(0, update)

    # ------------------------------------------------------------------
    # Payments
    # ------------------------------------------------------------------
    def _get_transaction_tuning(self) -> Dict[str, Optional[int]]:
        result: Dict[str, Optional[int]] = {"max_per_tx": None, "compute_unit_limit": None, "priority_fee": None}
        max_per_tx = self.max_per_tx_var.get().strip()
        if max_per_tx:
            try:
                value = int(max_per_tx)
            except ValueError:
                raise ValueError("El valor de 'Máx. wallets por transacción' debe ser un entero")
            if value < 1:
                raise ValueError("'Máx. wallets por transacción' debe ser al menos 1")
            result["max_per_tx"] = value
        compute_units = self.compute_unit_limit_var.get().strip()
        if compute_units:
            try:
                value = int(compute_units)
            except ValueError:
                raise ValueError("El límite de compute units debe ser un entero")
            if value < 25_000:
                raise ValueError("El límite de compute units debe ser al menos 25000")
            result["compute_unit_limit"] = value
        priority_fee = self.priority_fee_var.get().strip()
        if priority_fee:
            try:
                value = int(priority_fee)
            except ValueError:
                raise ValueError("La priority fee debe ser un entero (micro-lamports)")
            if value < 0:
                raise ValueError("La priority fee no puede ser negativa")
            result["priority_fee"] = value
        return result

    def _send_to_recipients(self) -> None:
        if not self.recipients:
            messagebox.showwarning("Envio", "Carga primero un archivo con destinatarios")
            return
        try:
            self.manager.get_wallet_address()
        except WalletNotLoadedError:
            messagebox.showwarning("Envio", "Carga primero la wallet madre")
            return
        try:
            tuning = self._get_transaction_tuning()
        except ValueError as exc:
            messagebox.showerror("Optimización", str(exc))
            return
        confirm = messagebox.askyesno(
            "Confirmar envíos",
            f"Se enviarán fondos a {len(self.recipients)} wallets. ¿Deseas continuar?",
        )
        if not confirm:
            return
        self._run_in_thread(self._send_payments_task, list(self.recipients), tuning)

    def _send_payments_task(self, recipients: List[Recipient], tuning: Dict[str, Optional[int]]) -> None:
        try:
            signatures = self.manager.send_mass_payments(
                recipients,
                max_per_transaction=tuning["max_per_tx"],
                compute_unit_limit=tuning["compute_unit_limit"],
                compute_unit_price_micro_lamports=tuning["priority_fee"],
            )
        except Exception as exc:
            self.log(f"Error al enviar pagos: {exc}")
            self._show_error("Envio", str(exc))
            return
        for signature in signatures:
            self.log(f"Transacción confirmada: {signature}")
        self._refresh_balance_async()
        self._refresh_recipient_balances_async()

    # ------------------------------------------------------------------
    # Async helpers & logging
    # ------------------------------------------------------------------
    def _run_in_thread(self, target, *args) -> None:
        def wrapper() -> None:
            self._set_busy(True)
            try:
                target(*args)
            finally:
                self._set_busy(False)
        threading.Thread(target=wrapper, daemon=True).start()

    def _run_background(self, target, *args) -> None:
        threading.Thread(target=target, args=args, daemon=True).start()

    def _set_busy(self, busy: bool) -> None:
        def update() -> None:
            state = "disabled" if busy else "normal"
            cursor = "watch" if busy else ""
            for button in self.action_buttons:
                button.configure(state=state)
            self.configure(cursor=cursor)
        self.after(0, update)

    def log(self, message: str) -> None:
        timestamp = time.strftime("%H:%M:%S")

        def append() -> None:
            self.log_text.configure(state="normal")
            self.log_text.insert("end", f"[{timestamp}] {message}\n")
            current_lines = int(float(self.log_text.index("end")))
            if current_lines > MAX_LOG_LINES:
                self.log_text.delete("1.0", "2.0")
            self.log_text.see("end")
            self.log_text.configure(state="disabled")

        self.after(0, append)

    def _show_error(self, title: str, message: str) -> None:
        self.after(0, lambda: messagebox.showerror(title, message))


def main() -> None:
    app = SolanaDistributorGUI()
    app.mainloop()


if __name__ == "__main__":
    main()
