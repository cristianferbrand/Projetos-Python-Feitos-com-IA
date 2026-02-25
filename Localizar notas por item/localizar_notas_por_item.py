# localizar_notas_por_item.py
# Lê um arquivo SPED (TXT) e lista (NUM_DOC, CHV_NFE) das notas (C100)
# que contenham ao menos um item (C170) com COD_ITEM igual ao informado.

from __future__ import annotations

import argparse
import csv
import os
import sys
from typing import List, Tuple, Set, Optional


def _norm_cod_item(value: str) -> str:
    """Normaliza COD_ITEM para comparação (remove espaços e zeros à esquerda se for numérico)."""
    v = (value or "").strip()
    if v.isdigit():
        return str(int(v))  # "000123" -> "123"
    return v


def _safe_fields(line: str) -> List[str]:
    """
    SPED geralmente vem como: |C100|...|  (pipe no começo e no fim).
    Retorna apenas os campos internos:
      '|C100|0|1|...' -> ['C100','0','1',...]
    """
    s = line.strip()
    if not s:
        return []
    if s.startswith("|"):
        s = s[1:]
    if s.endswith("|"):
        s = s[:-1]
    return s.split("|")


def _detect_encoding(path: str) -> str:
    """
    Detecta encoding testando decode em uma amostra do arquivo.
    Em SPED no BR é muito comum cp1252/latin-1.
    """
    with open(path, "rb") as bf:
        sample = bf.read(65536)

    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            sample.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue

    # fallback que nunca falha (1 byte -> 1 char)
    return "latin-1"


def _open_text_file(path: str):
    """
    Abre o arquivo no encoding detectado.
    errors='replace' garante que nunca vai quebrar por caractere inválido.
    """
    enc = _detect_encoding(path)
    return open(path, "r", encoding=enc, errors="replace")


def listar_notas_com_item(sped_path: str, cod_item_alvo: str) -> List[Tuple[str, str]]:
    """
    Retorna lista de (NUM_DOC, CHV_NFE) para cada C100 que tenha C170 com COD_ITEM == cod_item_alvo.
    """
    alvo = _norm_cod_item(cod_item_alvo)
    resultados: Set[Tuple[str, str]] = set()

    # Contexto do C100 corrente
    num_doc_atual: Optional[str] = None
    chave_atual: Optional[str] = None

    with _open_text_file(sped_path) as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue

            l = line.lstrip()
            if not (l.startswith("|C100|") or l.startswith("|C170|")):
                continue

            fields = _safe_fields(l)
            if not fields:
                continue

            reg = fields[0]

            if reg == "C100":
                # Pela tua amostra:
                # index 7 = NUM_DOC
                # index 8 = CHV_NFE
                num_doc_atual = fields[7].strip() if len(fields) > 7 else None
                chave_atual = fields[8].strip() if len(fields) > 8 else None

            elif reg == "C170":
                # COD_ITEM = fields[2] (ex: |C170|001|889842|...)
                cod_item = fields[2].strip() if len(fields) > 2 else ""
                if _norm_cod_item(cod_item) == alvo:
                    if num_doc_atual and chave_atual:
                        resultados.add((num_doc_atual, chave_atual))

    return sorted(resultados, key=lambda x: (x[0], x[1]))


def main():
    parser = argparse.ArgumentParser(
        description="Lista NUM_DOC e CHV_NFE (C100) das notas que contêm um COD_ITEM (C170)."
    )
    parser.add_argument("arquivo_sped", nargs="?", help="Caminho do arquivo TXT do SPED.")
    parser.add_argument("cod_item", nargs="?", default="889842", help="COD_ITEM para filtrar (padrão: 889842).")
    parser.add_argument("--csv", dest="csv_out", help="Se informado, salva também em CSV neste caminho.")
    args = parser.parse_args()

    sped_path = args.arquivo_sped
    cod_item = args.cod_item

    if not sped_path:
        try:
            import tkinter as tk
            from tkinter import filedialog

            root = tk.Tk()
            root.withdraw()
            sped_path = filedialog.askopenfilename(
                title="Selecione o arquivo TXT do SPED",
                filetypes=[("TXT", "*.txt"), ("Todos os arquivos", "*.*")]
            )
        except Exception as e:
            print("Erro ao abrir seletor de arquivo. Informe o caminho do arquivo na linha de comando.")
            print(f"Detalhes: {e}")
            sys.exit(2)

    if not sped_path or not os.path.isfile(sped_path):
        print("Arquivo SPED não encontrado. Verifique o caminho.")
        sys.exit(2)

    notas = listar_notas_com_item(sped_path, cod_item)

    print(f"\nArquivo: {sped_path}")
    print(f"Encoding detectado: {_detect_encoding(sped_path)}")
    print(f"COD_ITEM alvo: {cod_item}")
    print(f"Total de notas encontradas: {len(notas)}\n")

    for num, chave in notas:
        print(f"NUM_DOC={num} | CHV_NFE={chave}")

    if args.csv_out:
        with open(args.csv_out, "w", newline="", encoding="utf-8") as out:
            w = csv.writer(out, delimiter=";")
            w.writerow(["NUM_DOC", "CHV_NFE"])
            w.writerows(notas)
        print(f"\nCSV salvo em: {args.csv_out}")


if __name__ == "__main__":
    main()