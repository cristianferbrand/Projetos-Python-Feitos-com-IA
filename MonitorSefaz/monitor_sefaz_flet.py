# monitor_sefaz_flet_diag.py
# Monitor SEFAZ (NFe) – Dark Mode, A1 (.PFX), CA bundle, diagnóstico SSL/mTLS detalhado

import asyncio
import csv
import ssl
import tempfile
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

import httpx
import flet as ft
import certifi
from lxml import etree

# ---- cryptography (tolerante) ----
try:
    from cryptography import x509  # type: ignore
    from cryptography.hazmat.primitives.serialization import (  # type: ignore
        pkcs12, Encoding, NoEncryption, PrivateFormat
    )
    from cryptography.hazmat.backends import default_backend  # type: ignore
except Exception:
    x509 = pkcs12 = Encoding = NoEncryption = PrivateFormat = default_backend = None  # type: ignore


# ================= Compat Flet (C/I/op, Wrap) =================
def _resolve_flet_tokens():
    C = getattr(ft, "Colors", None)
    I = getattr(ft, "Icons", None)
    if C is not None and I is not None:
        def op(alpha: float, color):
            try:
                return color.with_opacity(alpha)
            except Exception:
                return color
        return C, I, op
    C = ft.colors
    I = ft.icons
    def op(alpha: float, color_str: str):
        try:
            return ft.colors.with_opacity(alpha, color_str)
        except Exception:
            return color_str
    return C, I, op

C, I, op = _resolve_flet_tokens()

def icon_or(name: str, fallback):
    try:
        return getattr(I, name)
    except Exception:
        return fallback

def make_wrap_container(controls=None, spacing=10, run_spacing=10):
    Wrap = getattr(ft, "Wrap", None)
    if Wrap is not None:
        return Wrap(controls=controls or [], spacing=spacing, run_spacing=run_spacing)
    return ft.Row(controls=controls or [], spacing=spacing, wrap=True)


# ================= Cabeçalhos e rótulos =================
DEFAULT_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/122.0.0.0 Safari/537.36"),
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "close",
}
LABELS = {"OK": "Disponível", "WARN": "Atenção", "DOWN": "Indisponível", "REQ_A1": "Requer A1", "NA": "Sem dados"}


# ================= UF / Endpoints =================
CUF = {
    "AC": 12, "AL": 27, "AM": 13, "AP": 16, "BA": 29, "CE": 23, "DF": 53, "ES": 32,
    "GO": 52, "MA": 21, "MG": 31, "MS": 50, "MT": 51, "PA": 15, "PB": 25, "PE": 26,
    "PI": 22, "PR": 41, "RJ": 33, "RN": 24, "RO": 11, "RR": 14, "RS": 43, "SC": 42,
    "SE": 28, "SP": 35, "TO": 17, "SVAN": 90, "SVRS": 43,
}
UFS_ALL = [
    "AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE",
    "PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"
]
UF_AUTORIZER = {
    "AM":"AM","BA":"BA","GO":"GO","MG":"MG","MS":"MS","MT":"MT","PE":"PE","PR":"PR","RS":"RS","SP":"SP",
    "AC":"SVRS","AL":"SVRS","AP":"SVRS","DF":"SVRS","ES":"SVRS","MA":"SVRS","PA":"SVRS","PB":"SVRS",
    "PI":"SVRS","RJ":"SVRS","RN":"SVRS","RO":"SVRS","RR":"SVRS","SC":"SVRS","SE":"SVRS","TO":"SVRS",
}

UF_SERVICES: Dict[str, Dict[str, Any]] = {
    "AM": {"autor": "AM", "cUF": CUF["AM"], "services": {
        "NfeInutilizacao":"https://nfe.sefaz.am.gov.br/services2/services/NfeInutilizacao4",
        "NfeConsultaProtocolo":"https://nfe.sefaz.am.gov.br/services2/services/NfeConsulta4",
        "NfeStatusServico":"https://nfe.sefaz.am.gov.br/services2/services/NfeStatusServico4",
        "NfeConsultaCadastro":"https://nfe.sefaz.am.gov.br/services2/services/CadConsultaCadastro4",
        "RecepcaoEvento":"https://nfe.sefaz.am.gov.br/services2/services/RecepcaoEvento4",
        "NFeAutorizacao":"https://nfe.sefaz.am.gov.br/services2/services/NfeAutorizacao4",
        "NFeRetAutorizacao":"https://nfe.sefaz.am.gov.br/services2/services/NfeRetAutorizacao4",
    }},
    "BA": {"autor": "BA", "cUF": CUF["BA"], "services": {
        "NfeInutilizacao":"https://nfe.sefaz.ba.gov.br/webservices/NFeInutilizacao4/NFeInutilizacao4.asmx",
        "NfeConsultaProtocolo":"https://nfe.sefaz.ba.gov.br/webservices/NFeConsultaProtocolo4/NFeConsultaProtocolo4.asmx",
        "NfeStatusServico":"https://nfe.sefaz.ba.gov.br/webservices/NFeStatusServico4/NFeStatusServico4.asmx",
        "NfeConsultaCadastro":"https://nfe.sefaz.ba.gov.br/webservices/CadConsultaCadastro4/CadConsultaCadastro4.asmx",
        "RecepcaoEvento":"https://nfe.sefaz.ba.gov.br/webservices/NFeRecepcaoEvento4/NFeRecepcaoEvento4.asmx",
        "NFeAutorizacao":"https://nfe.sefaz.ba.gov.br/webservices/NFeAutorizacao4/NFeAutorizacao4.asmx",
        "NFeRetAutorizacao":"https://nfe.sefaz.ba.gov.br/webservices/NFeRetAutorizacao4/NFeRetAutorizacao4.asmx",
    }},
    "GO": {"autor": "GO", "cUF": CUF["GO"], "services": {
        "NfeInutilizacao":"https://nfe.sefaz.go.gov.br/nfe/services/NFeInutilizacao4?wsdl",
        "NfeConsultaProtocolo":"https://nfe.sefaz.go.gov.br/nfe/services/NFeConsultaProtocolo4?wsdl",
        "NfeStatusServico":"https://nfe.sefaz.go.gov.br/nfe/services/NFeStatusServico4?wsdl",
        "NfeConsultaCadastro":"https://nfe.sefaz.go.gov.br/nfe/services/CadConsultaCadastro4?wsdl",
        "RecepcaoEvento":"https://nfe.sefaz.go.gov.br/nfe/services/NFeRecepcaoEvento4?wsdl",
        "NFeAutorizacao":"https://nfe.sefaz.go.gov.br/nfe/services/NFeAutorizacao4?wsdl",
        "NFeRetAutorizacao":"https://nfe.sefaz.go.gov.br/nfe/services/NFeRetAutorizacao4?wsdl",
    }},
    "MG": {"autor": "MG", "cUF": CUF["MG"], "services": {
        "NfeInutilizacao":"https://nfe.fazenda.mg.gov.br/nfe2/services/NFeInutilizacao4",
        "NfeConsultaProtocolo":"https://nfe.fazenda.mg.gov.br/nfe2/services/NFeConsultaProtocolo4",
        "NfeStatusServico":"https://nfe.fazenda.mg.gov.br/nfe2/services/NFeStatusServico4",
        "NfeConsultaCadastro":"https://nfe.fazenda.mg.gov.br/nfe2/services/CadConsultaCadastro4",
        "RecepcaoEvento":"https://nfe.fazenda.mg.gov.br/nfe2/services/NFeRecepcaoEvento4",
        "NFeAutorizacao":"https://nfe.fazenda.mg.gov.br/nfe2/services/NFeAutorizacao4",
        "NFeRetAutorizacao":"https://nfe.fazenda.mg.gov.br/nfe2/services/NFeRetAutorizacao4",
    }},
    "MS": {"autor": "MS", "cUF": CUF["MS"], "services": {
        "NfeInutilizacao":"https://nfe.sefaz.ms.gov.br/ws/NFeInutilizacao4",
        "NfeConsultaProtocolo":"https://nfe.sefaz.ms.gov.br/ws/NFeConsultaProtocolo4",
        "NfeStatusServico":"https://nfe.sefaz.ms.gov.br/ws/NFeStatusServico4",
        "NfeConsultaCadastro":"https://nfe.sefaz.ms.gov.br/ws/CadConsultaCadastro4",
        "RecepcaoEvento":"https://nfe.sefaz.ms.gov.br/ws/NFeRecepcaoEvento4",
        "NFeAutorizacao":"https://nfe.sefaz.ms.gov.br/ws/NFeAutorizacao4",
        "NFeRetAutorizacao":"https://nfe.sefaz.ms.gov.br/ws/NFeRetAutorizacao4",
    }},
    "MT": {"autor": "MT", "cUF": CUF["MT"], "services": {
        "NfeInutilizacao":"https://nfe.sefaz.mt.gov.br/nfews/v2/services/NfeInutilizacao4?wsdl",
        "NfeConsultaProtocolo":"https://nfe.sefaz.mt.gov.br/nfews/v2/services/NfeConsulta4?wsdl",
        "NfeStatusServico":"https://nfe.sefaz.mt.gov.br/nfews/v2/services/NfeStatusServico4?wsdl",
        "NfeConsultaCadastro":"https://nfe.sefaz.mt.gov.br/nfews/v2/services/CadConsultaCadastro4?wsdl",
        "RecepcaoEvento":"https://nfe.sefaz.mt.gov.br/nfews/v2/services/RecepcaoEvento4?wsdl",
        "NFeAutorizacao":"https://nfe.sefaz.mt.gov.br/nfews/v2/services/NfeAutorizacao4?wsdl",
        "NFeRetAutorizacao":"https://nfe.sefaz.mt.gov.br/nfews/v2/services/NfeRetAutorizacao4?wsdl",
    }},
    "PE": {"autor": "PE", "cUF": CUF["PE"], "services": {
        "NfeInutilizacao":"https://nfe.sefaz.pe.gov.br/nfe-service/services/NFeInutilizacao4",
        "NfeConsultaProtocolo":"https://nfe.sefaz.pe.gov.br/nfe-service/services/NFeConsultaProtocolo4",
        "NfeStatusServico":"https://nfe.sefaz.pe.gov.br/nfe-service/services/NFeStatusServico4",
        "NfeConsultaCadastro":"https://nfe.sefaz.pe.gov.br/nfe-service/services/CadConsultaCadastro4?wsdl",
        "RecepcaoEvento":"https://nfe.sefaz.pe.gov.br/nfe-service/services/NFeRecepcaoEvento4",
        "NFeAutorizacao":"https://nfe.sefaz.pe.gov.br/nfe-service/services/NFeAutorizacao4",
        "NFeRetAutorizacao":"https://nfe.sefaz.pe.gov.br/nfe-service/services/NFeRetAutorizacao4",
    }},
    "PR": {"autor": "PR", "cUF": CUF["PR"], "services": {
        "NfeInutilizacao":"https://nfe.sefa.pr.gov.br/nfe/NFeInutilizacao4?wsdl",
        "NfeConsultaProtocolo":"https://nfe.sefa.pr.gov.br/nfe/NFeConsultaProtocolo4?wsdl",
        "NfeStatusServico":"https://nfe.sefa.pr.gov.br/nfe/NFeStatusServico4?wsdl",
        "NfeConsultaCadastro":"https://nfe.sefa.pr.gov.br/nfe/CadConsultaCadastro4?wsdl",
        "RecepcaoEvento":"https://nfe.sefa.pr.gov.br/nfe/NFeRecepcaoEvento4?wsdl",
        "NFeAutorizacao":"https://nfe.sefa.pr.gov.br/nfe/NFeAutorizacao4?wsdl",
        "NFeRetAutorizacao":"https://nfe.sefa.pr.gov.br/nfe/NFeRetAutorizacao4?wsdl",
    }},
    "RS": {"autor": "RS", "cUF": CUF["RS"], "services": {
        "NfeInutilizacao":"https://nfe.sefazrs.rs.gov.br/ws/nfeinutilizacao/nfeinutilizacao4.asmx",
        "NfeConsultaProtocolo":"https://nfe.sefazrs.rs.gov.br/ws/NfeConsulta/NfeConsulta4.asmx",
        "NfeStatusServico":"https://nfe.sefazrs.rs.gov.br/ws/NfeStatusServico/NFeStatusServico4.asmx",
        "NfeConsultaCadastro":"https://cad.svrs.rs.gov.br/ws/cadconsultacadastro/cadconsultacadastro4.asmx",
        "RecepcaoEvento":"https://nfe.sefazrs.rs.gov.br/ws/recepcaoevento/recepcaoevento4.asmx",
        "NFeAutorizacao":"https://nfe.sefazrs.rs.gov.br/ws/NfeAutorizacao/NFeAutorizacao4.asmx",
        "NFeRetAutorizacao":"https://nfe.sefazrs.rs.gov.br/ws/NFeRetAutorizacao/NFeRetAutorizacao4.asmx",
    }},
    "SP": {"autor": "SP", "cUF": CUF["SP"], "services": {
        "NfeInutilizacao":"https://nfe.fazenda.sp.gov.br/ws/nfeinutilizacao4.asmx",
        "NfeConsultaProtocolo":"https://nfe.fazenda.sp.gov.br/ws/nfeconsultaprotocolo4.asmx",
        "NfeStatusServico":"https://nfe.fazenda.sp.gov.br/ws/nfestatusservico4.asmx",
        "NfeConsultaCadastro":"https://nfe.fazenda.sp.gov.br/ws/cadconsultacadastro4.asmx",
        "RecepcaoEvento":"https://nfe.fazenda.sp.gov.br/ws/nferecepcaoevento4.asmx",
        "NFeAutorizacao":"https://nfe.fazenda.sp.gov.br/ws/nfeautorizacao4.asmx",
        "NFeRetAutorizacao":"https://nfe.fazenda.sp.gov.br/ws/nferetautorizacao4.asmx",
    }},
    "SVAN": {"autor": "SVAN", "cUF": CUF["SVAN"], "services": {
        "NfeInutilizacao":"https://www.sefazvirtual.fazenda.gov.br/NFeInutilizacao4/NFeInutilizacao4.asmx",
        "NfeConsultaProtocolo":"https://www.sefazvirtual.fazenda.gov.br/NFeConsultaProtocolo4/NFeConsultaProtocolo4.asmx",
        "NfeStatusServico":"https://www.sefazvirtual.fazenda.gov.br/NFeStatusServico4/NFeStatusServico4.asmx",
        "RecepcaoEvento":"https://www.sefazvirtual.fazenda.gov.br/NFeRecepcaoEvento4/NFeRecepcaoEvento4.asmx",
        "NFeAutorizacao":"https://www.sefazvirtual.fazenda.gov.br/NFeAutorizacao4/NFeAutorizacao4.asmx",
        "NFeRetAutorizacao":"https://www.sefazvirtual.fazenda.gov.br/NFeRetAutorizacao4/NFeRetAutorizacao4.asmx",
    }},
    "SVRS": {"autor": "SVRS", "cUF": CUF["SVRS"], "services": {
        "NfeInutilizacao":"https://nfe.svrs.rs.gov.br/ws/nfeinutilizacao/nfeinutilizacao4.asmx",
        "NfeConsultaProtocolo":"https://nfe.svrs.rs.gov.br/ws/NfeConsulta/NfeConsulta4.asmx",
        "NfeStatusServico":"https://nfe.svrs.rs.gov.br/ws/NFeStatusServico/NFeStatusServico4.asmx",
        "NfeConsultaCadastro":"https://cad.svrs.rs.gov.br/ws/cadconsultacadastro/cadconsultacadastro4.asmx",
        "RecepcaoEvento":"https://nfe.svrs.rs.gov.br/ws/recepcaoevento/recepcaoevento4.asmx",
        "NFeAutorizacao":"https://nfe.svrs.rs.gov.br/ws/NfeAutorizacao/NFeAutorizacao4.asmx",
        "NFeRetAutorizacao":"https://nfe.svrs.rs.gov.br/ws/NFeRetAutorizacao/NFeRetAutorizacao4.asmx",
    }},
}

def ensure_wsdl(url: str) -> str:
    return url if url.endswith("?wsdl") else (url + "?wsdl")

def ensure_service(url: str) -> str:
    return url[:-5] if url.endswith("?wsdl") else url

def build_endpoints(tp_amb: str, status_wsdl_only: bool) -> List[Dict[str, Any]]:
    eps: List[Dict[str, Any]] = []
    for auth, info in UF_SERVICES.items():
        autor = info["autor"]; cuf = info["cUF"]
        for serv, base_url in info["services"].items():
            is_status = (serv.lower() == "nfestatusservico")
            probe_kind = "wsdl" if (status_wsdl_only and is_status) else ("status" if is_status else "wsdl")
            eps.append({
                "uf": auth, "autor": autor, "servico": serv,
                "ambiente": tp_amb, "cUF": cuf,
                "probe": probe_kind,
                "url_service": ensure_service(base_url),
                "url_wsdl": ensure_wsdl(base_url),
            })
    return eps


# ================= Util: diagnóstico de erros SSL/HTTPX =================
def _root_exc(e: BaseException) -> BaseException:
    cur = e
    while getattr(cur, "__cause__", None) is not None:
        cur = cur.__cause__  # type: ignore
    while getattr(cur, "__context__", None) is not None:
        cur = cur.__context__  # type: ignore
    return cur

def diagnose_ssl_error(e: Exception) -> str:
    """
    Retorna uma mensagem humana detalhando a causa mais provável.
    Cobertura: expirado, cadeia/CA ausente, hostname mismatch, TLS/versão,
    certificado autoassinado/revogado, timeouts, handshake, etc.
    """
    r = _root_exc(e)
    s = (str(r) or str(e)).lower()

    # Timeouts
    if isinstance(e, (httpx.ConnectTimeout, httpx.ReadTimeout)):
        return "Timeout de conexão/leitura (verifique rede, firewall ou endpoint)."

    # HTTPX conectivo
    if isinstance(e, (httpx.ConnectError, httpx.ProxyError)):
        # Pode carregar um SSLError como causa
        pass

    # Verificação de certificado (cadeia, expirado, hostname)
    if isinstance(r, ssl.SSLCertVerificationError):
        msg = getattr(r, "verify_message", "") or str(r)
        m = msg.lower()
        if "certificate has expired" in m or "expired" in m:
            return "Certificado expirado (cliente ou servidor). Renove o A1 ou ajuste o relógio do sistema."
        if "hostname" in m or "doesn't match" in m or "ip address mismatch" in m:
            return "Hostname do servidor não corresponde ao CN/SAN do certificado."
        if "unable to get local issuer certificate" in m or "unknown ca" in m:
            return "Cadeia de certificação incompleta/desconhecida. Adicione o CA intermediário (ICP-Brasil) no CA bundle."
        if "self signed certificate" in m:
            return "Certificado autoassinado detectado na cadeia. Use cadeia ICP-Brasil correta."
        if "certificate revoked" in m:
            return "Certificado revogado (CRL/OCSP). Utilize um A1 válido."
        return f"Falha na verificação de certificado: {msg}"

    # Erros SSL genéricos (protocolo/versão/cifra)
    if isinstance(r, ssl.SSLError):
        if "wrong version number" in s or "unsupported protocol" in s or "tlsv1 alert protocol version" in s:
            return "Negociação TLS falhou (versão incompatível). Os serviços exigem TLS 1.2+. Verifique proxies/inspeção SSL."
        if "dh key too small" in s:
            return "Falha de negociação: conjunto de cifras inseguro (DH muito fraca) no servidor."
        if "handshake failure" in s or "tlsv1 alert handshake failure" in s:
            return "Falha no handshake TLS. Se o serviço exige mTLS, habilite o A1 (.pfx) e a cadeia ICP-Brasil."
        if "alert certificate expired" in s:
            return "Certificado expirado informado durante o handshake."
        if "alert certificate unknown" in s:
            return "Certificado do cliente não reconhecido. Verifique A1 e cadeia ICP-Brasil."
        return f"Erro TLS/SSL: {str(r)}"

    # Outros casos comuns
    if "winerror 10060" in s or "timed out" in s:
        return "Timeout/porta inacessível (10060). Cheque firewall/roteamento."
    if "winerror 10061" in s or "actively refused" in s:
        return "Conexão recusada (10061). Serviço offline ou porta incorreta."
    if "certificate verify failed" in s and "local issuer" in s:
        return "Cadeia ausente. Forneça CA bundle (.pem) com os intermediários."
    # fallback
    return f"Erro na conexão: {type(r).__name__}: {str(r)}"


# ================= SSL a partir de PFX e inspeção =================
def inspect_pfx(pfx_path: str, password: Optional[str]) -> Dict[str, Any]:
    """Retorna info do certificado: assunto, emissor, validade, expirado, dias restantes."""
    if pkcs12 is None or x509 is None:
        return {"ok": False, "msg": "Pacote 'cryptography' não instalado."}
    with open(pfx_path, "rb") as f:
        pfx_data = f.read()
    key, cert, _ = pkcs12.load_key_and_certificates(
        pfx_data, None if not password else password.encode("utf-8")
    )
    if cert is None:
        return {"ok": False, "msg": "PFX inválido ou senha incorreta."}
    c: x509.Certificate = cert  # type: ignore
    not_before = c.not_valid_before.replace(tzinfo=timezone.utc)
    not_after  = c.not_valid_after.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    expired = now > not_after
    days_left = int((not_after - now).total_seconds() // 86400)
    subj = ", ".join(f"{n.oid._name}={n.value}" for n in c.subject)  # noqa
    iss  = ", ".join(f"{n.oid._name}={n.value}" for n in c.issuer)   # noqa
    return {
        "ok": True, "subject": subj, "issuer": iss,
        "not_before": str(not_before), "not_after": str(not_after),
        "expired": expired, "days_left": days_left
    }

def ssl_context_from_pfx(pfx_path: str, password: Optional[str], ca_bundle: Optional[str] = None) -> ssl.SSLContext:
    if pkcs12 is None:
        raise RuntimeError("O módulo 'cryptography' não está instalado. "
                           "Instale com: pip install --only-binary=:all: cryptography")
    with open(pfx_path, "rb") as f:
        pfx_data = f.read()
    key, cert, add_certs = pkcs12.load_key_and_certificates(
        pfx_data, None if not password else password.encode("utf-8")
    )
    if cert is None or key is None:
        raise ValueError("Arquivo PFX inválido ou senha incorreta.")

    cert_pem = cert.public_bytes(Encoding.PEM)
    chain_pem = b"".join(c.public_bytes(Encoding.PEM) for c in (add_certs or []))
    key_pem  = key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())

    cert_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pem")
    key_file  = tempfile.NamedTemporaryFile(delete=False, suffix=".key")
    cert_file.write(cert_pem + chain_pem); cert_file.close()
    key_file.write(key_pem); key_file.close()

    ctx = ssl.create_default_context(cafile=ca_bundle or certifi.where())
    try:
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2  # serviços exigem TLS 1.2+
    except Exception:
        pass
    ctx.load_cert_chain(certfile=cert_file.name, keyfile=key_file.name)
    return ctx


# ================= SOAP consStatServ 4.00 =================
def build_status_envelope(tp_amb: str, cuf: int) -> bytes:
    ns = "http://www.portalfiscal.inf.br/nfe"
    cons = etree.Element("{%s}consStatServ" % ns, versao="4.00", nsmap={None: ns})
    etree.SubElement(cons, "tpAmb").text = tp_amb
    etree.SubElement(cons, "cUF").text = str(cuf)
    etree.SubElement(cons, "xServ").text = "STATUS"
    soap_env = "http://www.w3.org/2003/05/soap-envelope"
    env = etree.Element("{%s}Envelope" % soap_env, nsmap={"soap12": soap_env})
    body = etree.SubElement(env, "{%s}Body" % soap_env)
    body.append(cons)
    return etree.tostring(env, xml_declaration=True, encoding="utf-8")

def parse_status_response(xml_text: str) -> Dict[str, Optional[str]]:
    out = {"cStat": None, "xMotivo": None, "dhRecbto": None, "tMed": None}
    try:
        root = etree.fromstring(xml_text.encode("utf-8"))
    except Exception:
        return out
    for el in root.iter():
        tag = el.tag
        if tag.endswith("cStat"):
            out["cStat"] = (el.text or "").strip()
        elif tag.endswith("xMotivo"):
            out["xMotivo"] = (el.text or "").strip()
        elif tag.endswith("dhRecbto"):
            out["dhRecbto"] = (el.text or "").strip()
        elif tag.endswith("tMed"):
            out["tMed"] = (el.text or "").strip()
    return out

def classify_status(cstat: Optional[str], http_ok: bool, exc: Optional[str]) -> str:
    if exc is not None:
        return "DOWN"
    if not http_ok:
        return "DOWN"
    if cstat == "107":
        return "OK"
    if cstat in {"108", "109"}:
        return "DOWN"
    return "WARN"


# ================= Probes =================
async def probe_status(client: httpx.AsyncClient, ep: Dict[str, Any]) -> Dict[str, Any]:
    t0 = time.perf_counter()
    url = ep["url_service"]; tp_amb = ep["ambiente"]; cuf = ep["cUF"]
    error = None; http_status = None; cstat = None; xmotivo = None
    headers = {"Content-Type": "application/soap+xml; charset=utf-8", **DEFAULT_HEADERS}
    data = build_status_envelope(tp_amb, cuf)
    status_label = "DOWN"
    try:
        resp = await client.post(url, content=data, headers=headers)
        http_status = resp.status_code
        if http_status == 403:
            status_label = "REQ_A1"
        if 200 <= http_status < 300:
            parsed = parse_status_response(resp.text)
            cstat = parsed.get("cStat"); xmotivo = parsed.get("xMotivo")
            status_label = classify_status(cstat, True, None)
        elif http_status and http_status != 403:
            error = f"HTTP {http_status}"
    except Exception as e:
        error = diagnose_ssl_error(e)
    elapsed = (time.perf_counter() - t0) * 1000.0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return {
        "uf": ep["uf"], "autor": ep["autor"], "servico": ep["servico"],
        "ambiente": "Produção" if tp_amb == "1" else "Homologação",
        "url": url, "latency_ms": round(elapsed, 1), "http": http_status,
        "cStat": cstat, "xMotivo": (xmotivo if not error else error),
        "status": status_label if not error else "DOWN",
        "error": error, "checked_at": now,
    }

async def probe_wsdl(client: httpx.AsyncClient, ep: Dict[str, Any]) -> Dict[str, Any]:
    t0 = time.perf_counter()
    url = ep["url_wsdl"]; error = None; http_status = None; klass = "DOWN"
    try:
        resp = await client.get(url, headers=DEFAULT_HEADERS)
        http_status = resp.status_code
        if http_status == 200: klass = "OK"
        elif http_status in (301, 302, 303, 307, 308, 405): klass = "WARN"
        else: klass = "DOWN"
    except Exception as e:
        error = diagnose_ssl_error(e)
    elapsed = (time.perf_counter() - t0) * 1000.0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return {
        "uf": ep["uf"], "autor": ep["autor"], "servico": ep["servico"],
        "ambiente": "Produção" if ep["ambiente"] == "1" else "Homologação",
        "url": url, "latency_ms": round(elapsed, 1), "http": http_status,
        "cStat": None, "xMotivo": (None if not error else error),
        "status": klass if not error else "DOWN",
        "error": error, "checked_at": now,
    }


# ================= App =================
def main(page: ft.Page):
    page.title = "Monitor SEFAZ – NFe (Dark) • A1"
    page.theme_mode = ft.ThemeMode.DARK
    page.scroll = ft.ScrollMode.AUTO

    # Paleta Dark
    BASE_BG   = "#0B0F14"
    SURF_1    = "#10151B"
    SURF_2    = "#141A22"
    SURF_3    = "#18212C"
    BORDER    = "#243040"
    ACCENT    = "#00B8D4"
    TEXT_2    = "#A7B3C2"

    CHIP_OK_BG   = "#0E3C3A"
    CHIP_WARN_BG = "#3B300E"
    CHIP_DOWN_BG = "#411B1B"
    CHIP_NA_BG   = "#1E2633"
    CHIP_REQ_BG  = "#2B2F12"  # Requer A1

    page.bgcolor = BASE_BG
    page.padding = 16
    page.horizontal_alignment = ft.CrossAxisAlignment.STRETCH
    page.vertical_alignment = ft.MainAxisAlignment.START

    running = False
    selected_uf = "RS"
    ambiente_atual = "1"
    wsdl_only_for_status = False

    cache: Dict[str, Dict[str, Any]] = {}
    endpoints: List[Dict[str, Any]] = build_endpoints(ambiente_atual, wsdl_only_for_status)

    # AppBar
    page.appbar = ft.AppBar(
        title=ft.Text("Monitor SEFAZ – NFe", weight=ft.FontWeight.BOLD, color=C.WHITE),
        leading=ft.Icon(I.SPEED, color=ACCENT),
        center_title=False,
        bgcolor=SURF_2,
        actions=[ft.IconButton(I.DARK_MODE, tooltip="Dark Mode", icon_color=ACCENT)],
    )

    # KPIs
    kpi_ok = ft.Text("0", size=28, weight=ft.FontWeight.BOLD, color=C.WHITE)
    kpi_warn = ft.Text("0", size=28, weight=ft.FontWeight.BOLD, color=C.WHITE)
    kpi_down = ft.Text("0", size=28, weight=ft.FontWeight.BOLD, color=C.WHITE)
    last_run_text = ft.Text("Parado.", color=TEXT_2)

    def kpi_box(title: str, value_ctrl: ft.Text, chip_bg: str) -> ft.Container:
        return ft.Container(
            bgcolor=chip_bg, border=ft.border.all(1, BORDER), border_radius=14, padding=12,
            content=ft.Column(spacing=6, controls=[ft.Text(title, size=12, color=TEXT_2), value_ctrl]),
        )

    kpi_row = ft.Row(
        spacing=10, wrap=True,
        controls=[
            kpi_box("Disponíveis", kpi_ok, CHIP_OK_BG),
            kpi_box("Atenção", kpi_warn, CHIP_WARN_BG),
            kpi_box("Indisponíveis", kpi_down, CHIP_DOWN_BG),
        ],
    )

    # ---------- Segurança/Certificados ----------
    sw_use_a1 = ft.Switch(label="Usar Certificado A1 (.PFX)", value=False)
    tf_pfx_path = ft.TextField(label="Caminho do .PFX", width=360, bgcolor=SURF_1, border_color=BORDER, color=C.WHITE)
    tf_pfx_pass = ft.TextField(label="Senha do .PFX", password=True, can_reveal_password=True, width=200,
                               bgcolor=SURF_1, border_color=BORDER, color=C.WHITE)

    fp_pfx = ft.FilePicker(on_result=lambda e: setattr(tf_pfx_path, "value", (e.files[0].path if e.files else tf_pfx_path.value)) or tf_pfx_path.update())
    page.overlay.append(fp_pfx)
    btn_pfx_pick = ft.ElevatedButton("Procurar…", icon=I.FOLDER_OPEN, bgcolor=ACCENT, color=C.WHITE,
                                     on_click=lambda e: fp_pfx.pick_files(allow_multiple=False))

    tf_ca_path = ft.TextField(label="CA bundle (.pem) – opcional", width=360, bgcolor=SURF_1, border_color=BORDER, color=C.WHITE)
    fp_ca = ft.FilePicker(on_result=lambda e: setattr(tf_ca_path, "value", (e.files[0].path if e.files else tf_ca_path.value)) or tf_ca_path.update())
    page.overlay.append(fp_ca)
    btn_ca_pick = ft.OutlinedButton(
        "Procurar PEM…",
        icon=icon_or("CERTIFICATE", I.SECURITY),  # fallback
        style=ft.ButtonStyle(color=C.WHITE),
        on_click=lambda e: fp_ca.pick_files(allow_multiple=False),
    )

    sw_ignore_ssl = ft.Switch(label="Ignorar validação SSL (debug)", value=False)

    # Diagnóstico A1 (mostra validade)
    diag_text = ft.Text("", color=TEXT_2, size=12)
    def run_pfx_diag(e=None):
        if not sw_use_a1.value or not (tf_pfx_path.value or "").strip():
            diag_text.value = "A1 desligado."
            diag_text.update()
            return
        info = inspect_pfx(tf_pfx_path.value.strip(), tf_pfx_pass.value or None)
        if not info.get("ok"):
            diag_text.value = f"Diagnóstico A1: {info.get('msg')}"
        else:
            expired = info["expired"]
            left = info["days_left"]
            end = info["not_after"]
            diag_text.value = f"Diagnóstico A1: {'EXPIRADO' if expired else f'válido, {left} dia(s) restante(s)'} • expira em {end}"
        diag_text.update()

    sw_use_a1.on_change = run_pfx_diag

    cert_row1 = ft.Row(wrap=True, spacing=10, controls=[sw_use_a1, tf_pfx_path, btn_pfx_pick, tf_pfx_pass])
    cert_row2 = ft.Row(wrap=True, spacing=10, controls=[tf_ca_path, btn_ca_pick, sw_ignore_ssl])
    cert_box = ft.Container(
        bgcolor=SURF_1, border=ft.border.all(1, BORDER), border_radius=12, padding=10,
        content=ft.Column(spacing=10, controls=[
            ft.Text("Certificados e Segurança", size=16, weight=ft.FontWeight.BOLD, color=C.WHITE),
            cert_row1, cert_row2, diag_text
        ])
    )

    # ---------- Filtros ----------
    dd_amb = ft.Dropdown(
        label="Ambiente", value="Produção",
        options=[ft.dropdown.Option("Produção"), ft.dropdown.Option("Homologação")],
        width=160, filled=True, bgcolor=SURF_1, border_color=BORDER, color=C.WHITE
    )
    sw_wsdl = ft.Switch(label="Modo rápido (WSDL-only p/ Status)", value=False)
    tf_interval = ft.TextField(label="Intervalo (s)", value="60", width=120, bgcolor=SURF_1, border_color=BORDER, color=C.WHITE)
    tf_conc = ft.TextField(label="Concorrência", value="16", width=140, bgcolor=SURF_1, border_color=BORDER, color=C.WHITE)
    tf_timeout = ft.TextField(label="Timeout (s)", value="15", width=120, bgcolor=SURF_1, border_color=BORDER, color=C.WHITE)
    bt_start = ft.ElevatedButton("Iniciar", icon=I.PLAY_ARROW, bgcolor=ACCENT, color=C.WHITE)
    bt_stop = ft.OutlinedButton("Parar", icon=I.STOP_CIRCLE, style=ft.ButtonStyle(color=C.WHITE))
    bt_export = ft.TextButton("Exportar CSV", icon=I.DOWNLOAD, style=ft.ButtonStyle(color=ACCENT))

    filters_row = ft.Row(
        wrap=True, spacing=10,
        controls=[dd_amb, sw_wsdl, tf_interval, tf_conc, tf_timeout, bt_start, bt_stop, bt_export],
    )
    filters_box = ft.Container(
        content=filters_row,
        bgcolor=SURF_1, border=ft.border.all(1, BORDER), border_radius=12, padding=10,
    )

    # ---------- Grid UFs ----------
    uf_wrap = make_wrap_container(spacing=8, run_spacing=8)
    uf_cards: Dict[str, ft.Container] = {}

    def status_to_colors(st: str) -> Tuple[str, str]:
        if st == "OK":      return (CHIP_OK_BG,   C.WHITE)
        if st == "WARN":    return (CHIP_WARN_BG, C.WHITE)
        if st == "DOWN":    return (CHIP_DOWN_BG, C.WHITE)
        if st == "REQ_A1":  return (CHIP_REQ_BG,  C.WHITE)
        return (CHIP_NA_BG, TEXT_2)

    def status_label(st: str) -> str:
        return LABELS.get(st, "Sem dados")

    def make_uf_card(uf: str) -> ft.Container:
        bg, fg = status_to_colors("NA")
        title = ft.Text(uf, size=16, weight=ft.FontWeight.BOLD, color=C.WHITE)
        subtitle = ft.Text("Sem dados", size=11, color=TEXT_2)
        chip = ft.Container(
            bgcolor=bg, border_radius=18, padding=ft.padding.symmetric(8, 4),
            content=ft.Text("Sem dados", size=11, color=fg, weight=ft.FontWeight.BOLD),
        )
        card = ft.Container(
            data={"title": title, "subtitle": subtitle, "chip": chip},
            width=120, height=86, padding=12, border_radius=14,
            bgcolor=SURF_1, border=ft.border.all(1, BORDER),
            on_click=lambda e, _uf=uf: on_select_uf(_uf),
            content=ft.Column(spacing=8, controls=[title, subtitle, chip]),
        )
        uf_cards[uf] = card
        return card

    def refresh_uf_grid():
        if not uf_wrap.controls:
            for uf in UFS_ALL:
                uf_wrap.controls.append(make_uf_card(uf))
        stats = latest_status_by_authorizer()
        ok = warn = down = 0
        for uf in UFS_ALL:
            auth = UF_AUTORIZER.get(uf, uf)
            info = stats.get(auth)
            st = info["status"] if info else "NA"
            lat = info.get("latency_ms") if info else None
            chip_bg, chip_fg = status_to_colors(st)
            label = status_label(st)
            uf_cards[uf].data["subtitle"].value = (f"{label}" if lat is None else f"{label} • {int(lat)}ms")
            uf_cards[uf].data["chip"].bgcolor = chip_bg
            uf_cards[uf].data["chip"].content = ft.Text(label, size=11, color=chip_fg, weight=ft.FontWeight.BOLD)
            if st == "OK": ok += 1
            elif st == "WARN": warn += 1
            elif st == "DOWN": down += 1
        kpi_ok.value, kpi_warn.value, kpi_down.value = str(ok), str(warn), str(down)
        kpi_ok.update(); kpi_warn.update(); kpi_down.update()
        uf_wrap.update()

    # ---------- Tabela (com rolagem) ----------
    columns = [
        ft.DataColumn(ft.Text("Autorizador", color=C.WHITE)),
        ft.DataColumn(ft.Text("Serviço", color=C.WHITE)),
        ft.DataColumn(ft.Text("Ambiente", color=C.WHITE)),
        ft.DataColumn(ft.Text("Latência (ms)", color=C.WHITE)),
        ft.DataColumn(ft.Text("HTTP", color=C.WHITE)),
        ft.DataColumn(ft.Text("cStat", color=C.WHITE)),
        ft.DataColumn(ft.Text("xMotivo / Erro", color=C.WHITE)),
        ft.DataColumn(ft.Text("Status", color=C.WHITE)),
        ft.DataColumn(ft.Text("Horário", color=C.WHITE)),
        ft.DataColumn(ft.Text("Endpoint", color=C.WHITE)),
    ]
    table = ft.DataTable(
        columns=columns, rows=[],
        heading_row_color= "#1A2532",
        data_row_color=  "#121922",
        divider_thickness=0.5,
        column_spacing=14,
        border=ft.border.all(1, BORDER),
    )
    table_scroller = ft.Container(
        bgcolor=SURF_1, border=ft.border.all(1, BORDER), border_radius=12, padding=10,
        height=420,
        content=ft.Column([table], scroll=ft.ScrollMode.AUTO),
    )

    def make_status_chip(st: str) -> ft.Container:
        bg, fg = status_to_colors(st)
        return ft.Container(
            bgcolor=bg, border_radius=12, padding=ft.padding.symmetric(8, 4),
            content=ft.Text(status_label(st), size=11, color=fg, weight=ft.FontWeight.BOLD),
        )

    def fill_table_for_selected_uf():
        table.rows.clear()
        auth = UF_AUTORIZER.get(selected_uf, selected_uf)
        for res in cache.values():
            if res["uf"] != auth:
                continue
            table.rows.append(
                ft.DataRow(cells=[
                    ft.DataCell(ft.Text(res["uf"], color=C.WHITE)),
                    ft.DataCell(ft.Text(res["servico"], color=C.WHITE)),
                    ft.DataCell(ft.Text(res["ambiente"], color=C.WHITE)),
                    ft.DataCell(ft.Text(str(res["latency_ms"]), color=C.WHITE)),
                    ft.DataCell(ft.Text(str(res["http"]) if res["http"] is not None else "-", color=C.WHITE)),
                    ft.DataCell(ft.Text(res["cStat"] or "-", color=C.WHITE)),
                    ft.DataCell(ft.Text(res["xMotivo"] or (res["error"] or "-"), color=C.WHITE)),
                    ft.DataCell(make_status_chip(res["status"])),
                    ft.DataCell(ft.Text(res["checked_at"], color=C.WHITE)),
                    ft.DataCell(ft.Text(res["url"], size=12, color=ACCENT)),
                ])
            )
        table.update()

    # Seleção UF
    selected_title = ft.Text(f"UF selecionada: {selected_uf}", size=16, weight=ft.FontWeight.BOLD, color=C.WHITE)
    def on_select_uf(uf: str):
        nonlocal selected_uf
        selected_uf = uf
        selected_title.value = f"UF selecionada: {selected_uf}"
        selected_title.update()
        fill_table_for_selected_uf()

    # Status por autorizador
    def latest_status_by_authorizer() -> Dict[str, Dict[str, Any]]:
        best: Dict[str, Dict[str, Any]] = {}
        for r in cache.values():
            if r.get("servico") != "NfeStatusServico":
                continue
            auth = r["uf"]
            prev = best.get(auth)
            if prev is None or (r.get("checked_at", "") > prev.get("checked_at", "")):
                best[auth] = r
        return best

    # Notificação
    def notify(msg: str):
        page.snack_bar = ft.SnackBar(
            content=ft.Text(msg, color=C.WHITE),
            show_close_icon=True, duration=2800, bgcolor=SUP3 if (SUP3:=None) else SURF_3,
        )
        page.snack_bar.open = True
        page.update()

    # HTTP client
    def create_http_client(timeout_seconds: int) -> httpx.AsyncClient:
        common = dict(timeout=timeout_seconds, headers=DEFAULT_HEADERS, http2=False)
        if sw_ignore_ssl.value:
            return httpx.AsyncClient(verify=False, **common)

        ca_path = (tf_ca_path.value or "").strip() or None
        if sw_use_a1.value:
            pfx_path = (tf_pfx_path.value or "").strip()
            pfx_pass = tf_pfx_pass.value or None
            # pré-diagnóstico A1
            info = inspect_pfx(pfx_path, pfx_pass)
            if info.get("ok") and info.get("expired"):
                notify("A1 EXPIRADO – renove o certificado para comunicar com a SEFAZ.")
            ctx = ssl_context_from_pfx(pfx_path, pfx_pass, ca_bundle=ca_path or certifi.where())
            return httpx.AsyncClient(verify=ctx, **common)
        else:
            verify = ca_path or certifi.where()
            return httpx.AsyncClient(verify=verify, **common)

    # Loop
    async def poll_loop():
        nonlocal running
        running = True
        bt_start.disabled = True; bt_stop.disabled = False
        bt_start.update(); bt_stop.update()
        last_run_text.value = "Executando..."
        last_run_text.color = C.WHITE
        last_run_text.update()

        try:
            to_s = max(3, int(tf_timeout.value or "15"))
        except Exception:
            to_s = 15

        while running:
            try:
                try:
                    delay = max(5, int(tf_interval.value or "60"))
                except Exception:
                    delay = 60
                try:
                    concurrency = max(1, int(tf_conc.value or "16"))
                except Exception:
                    concurrency = 16

                async with create_http_client(to_s) as client:
                    sem = asyncio.Semaphore(concurrency)

                    async def _task(ep):
                        async with sem:
                            return await (probe_status(client, ep) if ep["probe"]=="status" else probe_wsdl(client, ep))

                    tasks = [asyncio.create_task(_task(ep)) for ep in endpoints]
                    completed = 0
                    for coro in asyncio.as_completed(tasks):
                        res = await coro
                        key = f'{res["uf"]}|{res["servico"]}|{res["ambiente"]}'
                        prev = cache.get(key)
                        cache[key] = res
                        completed += 1
                        if prev is not None and prev.get("status") != res.get("status") and res["servico"] == "NfeStatusServico":
                            notify(f'{res["uf"]}: {LABELS.get(res["status"], res["status"])}')

                refresh_uf_grid()
                fill_table_for_selected_uf()
                last_run_text.value = f'Última rodada: {datetime.now().strftime("%H:%M:%S")} • Itens: {completed}'
                last_run_text.color = TEXT_2
                last_run_text.update()

                for _ in range(delay):
                    if not running:
                        break
                    await asyncio.sleep(1)

            except Exception as e:
                last_run_text.value = f"Erro no loop: {diagnose_ssl_error(e)}"
                last_run_text.color = C.WHITE
                last_run_text.update()
                await asyncio.sleep(5)

        bt_start.disabled = False; bt_stop.disabled = True
        bt_start.update(); bt_stop.update()
        last_run_text.value = "Parado."
        last_run_text.color = TEXT_2
        last_run_text.update()

    # Handlers
    def do_start(_):
        run_pfx_diag()
        page.run_task(poll_loop)
    def do_stop(_):
        nonlocal running
        running = False
    def do_export(_):
        path = f"sefaz_monitor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["Autorizador","Serviço","Ambiente","Latência_ms","HTTP","cStat","xMotivo/Erro","Status","Horário","Endpoint"])
            for r in cache.values():
                w.writerow([r["uf"], r["servico"], r["ambiente"], r["latency_ms"],
                            r["http"] or "", r["cStat"] or "", r["xMotivo"] or (r["error"] or ""),
                            r["status"], r["checked_at"], r["url"]])
        notify(f"Exportado: {path}")
    def on_amb_change(e):
        nonlocal ambiente_atual, endpoints
        ambiente_atual = "1" if dd_amb.value == "Produção" else "2"
        endpoints = build_endpoints(ambiente_atual, wsdl_only_for_status)
        notify(f"Ambiente: {dd_amb.value}")
    def on_wsdl_mode_change(e):
        nonlocal wsdl_only_for_status, endpoints
        wsdl_only_for_status = bool(sw_wsdl.value)
        endpoints = build_endpoints(ambiente_atual, wsdl_only_for_status)
        notify("Modo rápido ativo" if wsdl_only_for_status else "Modo completo (SOAP)")

    bt_start.on_click = do_start
    bt_stop.on_click = do_stop
    bt_export.on_click = do_export
    dd_amb.on_change = on_amb_change
    sw_wsdl.on_change = on_wsdl_mode_change

    # ---------- Layout ----------
    page.add(
        ft.Container(content=ft.Row([ft.Text("KPIs", size=16, weight=ft.FontWeight.BOLD, color=C.WHITE)])),
        ft.Container(content=ft.Row(controls=[kpi_row]), bgcolor= "#0F141B", border=ft.border.all(1, BORDER), border_radius=12, padding=10),
        ft.Container(height=10),
        cert_box,
        ft.Container(height=10),
        filters_box,
        ft.Container(height=12),
        ft.Row([ft.Text("Status por UF", size=16, weight=ft.FontWeight.BOLD, color=C.WHITE)]),
        uf_wrap,
        ft.Container(height=14),
        ft.Container(content=ft.Text(f"UF selecionada: {selected_uf}", size=16, weight=ft.FontWeight.BOLD, color=C.WHITE)),
        table_scroller,
        ft.Container(height=6),
        ft.Container(content=last_run_text),
    )

    # Inicial
    refresh_uf_grid()
    fill_table_for_selected_uf()


if __name__ == "__main__":
    ft.app(target=main)