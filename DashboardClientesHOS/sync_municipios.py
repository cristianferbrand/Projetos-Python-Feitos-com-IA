#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sync_municipios.py (Pylance-friendly)
-------------------------------------
Sincroniza municipios.csv (18 colunas) para a tabela public.municipios (5 colunas: municipio, uf, lon, lat, name).

- Mantém municipio como CHAR(7) (zero-padded).
- Faz COPY para staging (_municipios_stage) e depois UPSERT na final (public.municipios).
- Usa psycopg (v3) se disponível; cai para psycopg2 (v2) se necessário.
- Suporte a --encoding com fallback automático (utf-8 → utf-8-sig → cp1252 → latin-1).
- Opção --client-encoding para ajustar a sessão (ex.: UTF8).
- Comentários "pyright: ignore[reportMissingImports]" para silenciar o alerta do Pylance quando as libs opcionais não estiverem instaladas no ambiente atual.

Uso (exemplo):
  python -u sync_municipios.py --csv "C:/Users/Administrator/Desktop/MapaCliente/municipios.csv" --dsn "host=127.0.0.1 port=5432 dbname=seu_db user=hos_app password=hos26214400" --encoding auto --client-encoding UTF8

Requisitos:
  pip install psycopg[binary]     # (recomendado)
  # ou
  pip install psycopg2-binary
"""

import argparse
import os
import sys
import io
from contextlib import closing

def _log(msg: str) -> None:
    print(str(msg), file=sys.stderr, flush=True)

# Prefer psycopg (v3), com fallback para psycopg2 (v2)
try:
    import psycopg  # pyright: ignore[reportMissingImports]
    HAVE_PG3 = True
    _log("[INFO] Driver: psycopg v3 detectado")
except Exception:
    psycopg = None  # type: ignore[assignment]
    HAVE_PG3 = False

if not HAVE_PG3:
    try:
        import psycopg2  # pyright: ignore[reportMissingImports]
        HAVE_PG2 = True
        _log("[INFO] Driver: psycopg2 (v2) detectado (fallback)")
    except Exception:
        HAVE_PG2 = False
        _log("[ERRO] Nenhum driver do PostgreSQL disponível. Instale psycopg[binary] ou psycopg2-binary.")

DDL_STAGE = """
CREATE TEMP TABLE _municipios_stage (
    municipio         TEXT,
    uf                TEXT,
    uf_code           TEXT,
    name              TEXT,
    mesoregion        TEXT,
    microregion       TEXT,
    rgint             TEXT,
    rgi               TEXT,
    osm_relation_id   TEXT,
    wikidata_id       TEXT,
    is_capital        TEXT,
    wikipedia_pt      TEXT,
    lon               TEXT,
    lat               TEXT,
    no_accents        TEXT,
    slug_name         TEXT,
    alternative_names TEXT,
    pop_21            TEXT
);
"""

UPSERT_SQL = r"""
WITH norm AS (
  SELECT
    LPAD(regexp_replace(COALESCE(municipio,''), '\D', '', 'g'), 7, '0')::CHAR(7) AS municipio,
    NULLIF(BTRIM(uf, ' '), '')                           AS uf,
    NULLIF(BTRIM(name, ' '), '')                         AS name,
    NULLIF(REPLACE(lon, ',', '.'), '')::DOUBLE PRECISION AS lon,
    NULLIF(REPLACE(lat, ',', '.'), '')::DOUBLE PRECISION AS lat
  FROM _municipios_stage
  WHERE COALESCE(municipio,'') <> ''
)
INSERT INTO public.municipios (municipio, uf, lon, lat, name)
SELECT municipio, uf, lon, lat, name
FROM norm
ON CONFLICT (municipio) DO UPDATE SET
  uf  = EXCLUDED.uf,
  lon = EXCLUDED.lon,
  lat = EXCLUDED.lat,
  name= EXCLUDED.name;
"""

def read_csv_to_io(csv_path: str, encoding: str = "auto") -> io.StringIO:
    """
    Lê o CSV e retorna um StringIO normalizado para COPY STDIN.
    encoding = "auto" tenta: utf-8, utf-8-sig, cp1252, latin-1 (nesta ordem).
    """
    enc = (encoding or "auto").lower().strip()
    enc_list = ["utf-8", "utf-8-sig", "cp1252", "latin-1"] if enc == "auto" else [enc]

    last_exc = None
    for enc_try in enc_list:
        try:
            with open(csv_path, "r", encoding=enc_try, newline="") as f:
                data = f.read()
            _log(f"[INFO] CSV lido como {enc_try}")
            break
        except UnicodeDecodeError as e:
            last_exc = e
            continue
        except FileNotFoundError as e:
            _log(f"[ERRO] CSV não encontrado: {csv_path}")
            raise
    else:
        # Nenhuma codificação funcionou
        raise last_exc if last_exc else UnicodeDecodeError("utf-8", b"", 0, 1, "unknown decode error")

    # Normaliza quebras de linha (evita problemas em COPY)
    data = data.replace("\r\n", "\n").replace("\r", "\n")
    return io.StringIO(data)

DDL_AND_COPY_COLUMNS = """
    municipio, uf, uf_code, name, mesoregion, microregion, rgint, rgi,
    osm_relation_id, wikidata_id, is_capital, wikipedia_pt, lon, lat,
    no_accents, slug_name, alternative_names, pop_21
"""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Caminho para municipios.csv (18 colunas)")
    ap.add_argument("--dsn", default=os.environ.get("POSTGRES_DSN", ""), help="DSN do PostgreSQL (ex.: 'host=127.0.0.1 port=5432 dbname=... user=... password=...')")
    ap.add_argument("--encoding", default="auto", help="Codificação do CSV (utf-8, utf-8-sig, cp1252, latin-1, auto)")
    ap.add_argument("--client-encoding", default=None, help="Opcional: força client_encoding na sessão (ex.: UTF8, LATIN1).")
    args = ap.parse_args()

    _log(f"[INFO] Script: {__file__}")
    _log(f"[INFO] CSV: {args.csv}")
    _log(f"[INFO] Encoding: {args.encoding}")
    if args.client_encoding:
        _log(f"[INFO] client_encoding: {args.client_encoding}")

    if not os.path.exists(args.csv):
        _log(f"[ERRO] CSV não encontrado: {args.csv}")
        sys.exit(1)

    if not args.dsn:
        _log("[ERRO] Informe --dsn ou defina POSTGRES_DSN no ambiente")
        sys.exit(2)

    buf = read_csv_to_io(args.csv, args.encoding)

    if 'HAVE_PG3' in globals() and HAVE_PG3:
        # psycopg v3
        try:
            with psycopg.connect(args.dsn) as conn:  # pyright: ignore[reportMissingImports]
                if args.client_encoding:
                    conn.execute(f"SET client_encoding TO '{args.client_encoding}';")

                conn.execute("BEGIN;")
                conn.execute(DDL_STAGE)

                copy_sql = f"""
                    COPY _municipios_stage (
                        {DDL_AND_COPY_COLUMNS}
                    )
                    FROM STDIN WITH (FORMAT csv, HEADER true)
                """
                with conn.cursor() as cur:  # pyright: ignore[reportMissingImports]
                    cur.copy(copy_sql, buf)

                conn.execute(UPSERT_SQL)
                conn.execute("COMMIT;")
                _log("[OK] Sync concluído com psycopg v3.")
        except Exception as e:
            _log(f"[ERRO] psycopg v3: {e}")
            try:
                conn.execute("ROLLBACK;")  # pyright: ignore[reportMissingImports]
            except Exception:
                pass
            sys.exit(3)

    elif 'HAVE_PG2' in globals() and HAVE_PG2:
        # psycopg2
        try:
            import psycopg2  # pyright: ignore[reportMissingImports]
            with closing(psycopg2.connect(args.dsn)) as conn:
                conn.autocommit = False
                with conn.cursor() as cur:
                    if args.client_encoding:
                        cur.execute(f"SET client_encoding TO '{args.client_encoding}';")

                    cur.execute(DDL_STAGE)

                    cur.copy_expert(f"""
                        COPY _municipios_stage (
                            {DDL_AND_COPY_COLUMNS}
                        )
                        FROM STDIN WITH CSV HEADER
                    """, buf)

                    cur.execute(UPSERT_SQL)

                conn.commit()
                _log("[OK] Sync concluído com psycopg2.")
        except Exception as e:
            _log(f"[ERRO] psycopg2: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
            sys.exit(4)
    else:
        _log("Instale psycopg (>=3) ou psycopg2-binary.")
        sys.exit(5)

if __name__ == "__main__":
    main()
