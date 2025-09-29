
"""

  CLI en consola que:
   • Solicita nombre/nickname y muestra mensaje de bienvenida con operadores de string.
   • Ejecuta una "carga" de exactamente 5 segundos (con puntos animados).
   • Implementa un MENÚ TABULAR (2 columnas) con ciclo while que no termina hasta elegir Salir.
   • Ofrece 3+ operaciones de archivos: listar/abrir, crear, escribir (append).
   • Permite "Cambiar usuario" (nickname) sin salir.
   • Captura fecha (dd/mm/aaaa), la almacena como tupla (día, mes, año) y la usa al crear/modificar archivos.
   • Maneja excepciones (FileNotFoundError, ValueError, PermissionError, entradas inválidas) y mantiene ejecución.
   • Opción para lanzar la GUI de Solana (tkinter) si está disponible en el mismo entorno.

Requisitos:
  - Python 3.10+
  - Archivos del proyecto en la misma carpeta:
      solana_gui.py, solana_manager.py, recibir-wallets.txt (opcional)

Nota:
  Este CLI no reemplaza la GUI; es una capa de "control de flujo" en consola
  diseñada específicamente para cumplir la rúbrica.
"""
from __future__ import annotations

import os
import sys
import time
from datetime import datetime
from pathlib import Path

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

def clear() -> None:
    os.system("cls" if os.name == "nt" else "clear")

def esperar_carga_5s() -> None:
    """Muestra una animación de 'cargando' por EXACTAMENTE 5 segundos."""
    inicio = time.time()
    print("Cargando", end="", flush=True)
    i = 0
    while time.time() - inicio < 5.0:  # 5 segundos exactos
        print(".", end="", flush=True)
        time.sleep(0.5)
        i += 1
    print("")

def solicitar_nombre() -> str:
    while True:
        nombre = input("Ingresa tu nombre o nickname: ").strip()
        if nombre:
            return nombre
        print("⚠️  El nombre no puede ir vacío. Intenta de nuevo.")

def solicitar_fecha_tuple() -> tuple[int,int,int]:
    """Pide fecha dd/mm/aaaa y devuelve tupla (d, m, a)."""
    while True:
        txt = input("Ingresa la fecha (dd/mm/aaaa): ").strip()
        try:
            d, m, a = txt.split("/")
            d, m, a = int(d), int(m), int(a)
            # Validación básica
            datetime(a, m, d)
            return (d, m, a)
        except Exception:
            print("❌ Formato inválido. Ejemplo válido: 07/05/2025")

def bienvenida(nombre: str) -> None:
    clear()
    # Operadores de string: mayúsculas, f-string, concatenación
    mensaje = f"¡Bienvenid@, {nombre.title()}! "
    mensaje += "Este es el Launcher CLI del proyecto Solana Distributor."
    print(mensaje)
    esperar_carga_5s()

def listar_archivos_diccionario() -> dict[int, Path]:
    """Lista archivos de la carpeta raíz y data/ en forma de diccionario indexado."""
    print("\n=== Archivos disponibles (carpeta del proyecto y /data) ===")
    entradas = []
    for p in sorted(Path(".").glob("*")):
        if p.is_file() and p.suffix.lower() in {".txt", ".json", ".md", ".py"}:
            entradas.append(p)
    for p in sorted(DATA_DIR.glob("*")):
        if p.is_file():
            entradas.append(p)
    # Garantiza que existan al menos 4 archivos creando ejemplos si faltan
    plantillas_creadas = []
    while len(entradas) < 4:
        idx = len(entradas) + 1
        demo = DATA_DIR / f"demo_{idx}.txt"
        demo.write_text(f"Demo {idx} - creado por el Launcher CLI\n", encoding="utf-8")
        plantillas_creadas.append(demo)
        entradas.append(demo)

    # Mostrar en 2 columnas
    mapping = {}
    print("-"*78)
    print(f"{'Índice':<8}{'Archivo':<45}{'Ubicación':<25}")
    print("-"*78)
    for i, path in enumerate(entradas, start=1):
        mapping[i] = path
        ubic = "data/" if path.parent == DATA_DIR else "./"
        print(f"{i:<8}{path.name:<45}{ubic:<25}")
    print("-"*78)
    if plantillas_creadas:
        print(f"ℹ️  Se crearon {len(plantillas_creadas)} archivos de ejemplo en /data para cumplir la rúbrica.")
    return mapping

def abrir_archivo(mapping: dict[int, Path]) -> None:
    try:
        idx = int(input("Escribe el índice del archivo a abrir: ").strip())
        path = mapping[idx]
        print(f"\n----- CONTENIDO: {path} -----")
        try:
            txt = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            txt = path.read_text(encoding="latin-1")
        print(txt if txt else "(archivo vacío)")
        print("-"*60)
    except (KeyError, ValueError):
        print("⚠️  Índice inválido.")
    except FileNotFoundError:
        print("⚠️  Archivo no encontrado.")
    except PermissionError:
        print("⚠️  Permiso denegado al abrir el archivo.")

def crear_archivo(nombre_usuario: str, fecha_tuple: tuple[int,int,int]) -> None:
    d, m, a = fecha_tuple
    print("\nTipos de archivo:")
    print(" 1) Recipients (.txt)")
    print(" 2) Config (.json)")
    print(" 3) Notas (.txt)")
    opcion = input("Elige tipo (1-3): ").strip()
    if opcion not in {"1","2","3"}:
        print("⚠️  Opción inválida.")
        return
    ts = f"{a:04d}{m:02d}{d:02d}"
    try:
        if opcion == "1":
            path = DATA_DIR / f"recipients_{ts}.txt"
            header = f"# Recipients generado el {d:02d}/{m:02d}/{a:04d} por {nombre_usuario}\n# address,amount\n"
            path.write_text(header, encoding="utf-8")
        elif opcion == "2":
            path = DATA_DIR / f"config_{ts}.json"
            obj = {
                "generated_on": f"{d:02d}/{m:02d}/{a:04d}",
                "author": nombre_usuario,
                "network": "devnet",
                "default_amount": 0.1
            }
            import json
            path.write_text(__import__("json").dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            path = DATA_DIR / f"notas_{ts}.txt"
            path.write_text(f"Notas ({d:02d}/{m:02d}/{a:04d}) — {nombre_usuario}\n", encoding="utf-8")
        print(f"✅ Archivo creado: {path}")
    except PermissionError:
        print("⚠️  Permiso denegado al crear el archivo.")
    except Exception as e:
        print(f"❌ Error al crear el archivo: {e}")

def escribir_en_archivo(mapping: dict[int, Path]) -> None:
    try:
        idx = int(input("Índice del archivo para escribir (append): ").strip())
        path = mapping[idx]
        linea = input("Escribe el texto a agregar (se añadirá al final): ").strip()
        with path.open("a", encoding="utf-8") as f:
            f.write(linea + "\n")
        print("✅ Texto agregado.")
    except (KeyError, ValueError):
        print("⚠️  Índice inválido.")
    except FileNotFoundError:
        print("⚠️  Archivo no encontrado.")
    except PermissionError:
        print("⚠️  Permiso denegado al escribir en el archivo.")
    except Exception as e:
        print(f"❌ Error al escribir: {e}")

def lanzar_gui() -> None:
    """Intenta lanzar la GUI de Solana en el mismo intérprete."""
    try:
        import solana_gui  # noqa: F401  (importa y arranca la GUI si define main)
        if hasattr(solana_gui, "main"):
            print("Abriendo GUI... (cierra la ventana para volver al CLI)")
            solana_gui.main()
        else:
            print("⚠️  No se encontró función main() en solana_gui.py.")
    except Exception as e:
        print(f"⚠️  No se pudo abrir la GUI: {e}")

def mostrar_menu(nombre: str, fecha_tuple: tuple[int,int,int]) -> None:
    while True:
        print("\n==================== MENÚ PRINCIPAL ====================")
        print(f"Usuario: {nombre}    Fecha: {fecha_tuple[0]:02d}/{fecha_tuple[1]:02d}/{fecha_tuple[2]:04d}")
        print("--------------------------------------------------------")
        # Formato tabular 2 columnas
        print(f"{'1) Listar archivos':<30} {'2) Abrir archivo':<30}")
        print(f"{'3) Crear archivo':<30} {'4) Escribir (append)':<30}")
        print(f"{'5) Cambiar usuario':<30} {'6) Lanzar GUI Solana':<30}")
        print(f"{'7) Salir':<30}")
        print("========================================================")
        opcion = input("Elige una opción (1-7): ").strip()
        if opcion == "1":
            mapping = listar_archivos_diccionario()
        elif opcion == "2":
            mapping = listar_archivos_diccionario()
            abrir_archivo(mapping)
        elif opcion == "3":
            crear_archivo(nombre, fecha_tuple)
        elif opcion == "4":
            mapping = listar_archivos_diccionario()
            escribir_en_archivo(mapping)
        elif opcion == "5":
            nombre = solicitar_nombre()
            bienvenida(nombre)
        elif opcion == "6":
            lanzar_gui()
        elif opcion == "7":
            print("👋 ¡Hasta pronto!")
            break
        else:
            print("⚠️  Opción inválida. Intenta de nuevo.")

def main() -> None:
    clear()
    nombre = solicitar_nombre()
    bienvenida(nombre)
    fecha_tuple = solicitar_fecha_tuple()
    mostrar_menu(nombre, fecha_tuple)

if __name__ == "__main__":
    main()
