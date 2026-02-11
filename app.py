"""
ESCO Aranceles WebApp (FULL) + Login Web
---------------------------------------
- Login en /login (no hardcodea contraseña)
- Guarda credenciales en session (RAM del navegador) y token en memoria del server
- Lee Aranceles.xlsx
- Para cada (Grupo, Operación(Abrev), Especie, Moneda) toma la fila con Fecha de Vigencia más reciente
- Dado un comitente/cuenta, consulta ESCO get-detalle-cuenta y busca CodGrupoArOperBurs
- UI con selects en cascada (server-side)

Requisitos:
  pip install flask requests pandas openpyxl

Ejecución:
  set ESCO_BASE_URL=http://190.210.249.97:8003
  set ESCO_API_VERSION=9
  set FLASK_SECRET=algo_largo
  python app.py

Luego abrir:
  http://127.0.0.1:5000  (redirige a /login si no hay sesión)
"""

import os
import time
import json
from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple, List

import pandas as pd
import requests
from flask import Flask, render_template, request, flash, redirect, url_for, session


# ---------------- Config ----------------
DEFAULT_EXCEL_PATH = os.environ.get("ARANCELES_XLSX", "Aranceles.xlsx")
# Google Sheets (opciones)
# 1) Setear ARANCELES_SHEET_ID (id del doc) y opcional ARANCELES_SHEET_GID (gid de la pestaña, default 0)
# 2) O setear ARANCELES_SHEET_CSV_URL directamente (link export?format=csv&gid=...)
ARANCELES_SHEET_ID = os.environ.get("ARANCELES_SHEET_ID", "").strip()
ARANCELES_SHEET_GID = os.environ.get("ARANCELES_SHEET_GID", "0").strip()
ARANCELES_SHEET_CSV_URL = os.environ.get("ARANCELES_SHEET_CSV_URL", "").strip()


ESCO_BASE_URL = os.environ.get("ESCO_BASE_URL", "http://190.210.249.97:8003").rstrip("/")
ESCO_API_VERSION = os.environ.get("ESCO_API_VERSION", "9")

USE_ODATA_HEADERS = True  # replica swagger


# Mapeo: código -> descripción (tu lista)
GRUPOS_MAP = {
    1: "Standard",
    2: "Cartera Propia",
    3: "Grupo FCI",
    4: "Grupo 4 NA",
    5: "Grupo 5 IP",
    6: "Grupo 6 PM",
    7: "Grupo 7 PM Personal",
    8: "Grupo Contrapartes",
    9: "Grupo  Ctte 2020",
    10: "Grupo Ctte 2114",
    11: "Grupo Cartera Ppia ampliada",
    12: "Grupo Ctte 3321",
    13: "Grupo Standard Caucion 2.5",
    14: "Grupo Ctte 3484",
    15: "Grupo Ctte 2766",
    17: "Comitentes Corti",
    18: "Grupo 4% Caucion",
    19: "Grupo Cordoba",
    20: "Grupo Ctte 3633",
    21: "Grupo Ctte 2634",
    22: "Grupo 5% Caucion",
    23: "Grupo Op 0.25%",
    24: "Grupo 3500 2.25% caución",
    25: "Grupo 1.5% Caución",
    26: "Grupo Ctte 3732",
}


# ---------------- ESCO Client ----------------
@dataclass
class TokenState:
    token: Optional[str] = None
    exp_epoch: float = 0.0  # best-effort

    def valid(self) -> bool:
        return bool(self.token) and time.time() < (self.exp_epoch - 30)


class EscoClient:
    def __init__(self):
        self.base_url = ESCO_BASE_URL
        self.api_version = ESCO_API_VERSION
        self.username = ""
        self.password = ""
        self.client_id = ""
        self.state = TokenState()

    def set_creds(self, username: str, password: str, client_id: str = ""):
        self.username = username or ""
        self.password = password or ""
        self.client_id = client_id or ""

    def _headers(self, accept_json=True, send_json=False) -> Dict[str, str]:
        h = {"api-version": str(self.api_version)}
        if self.state.token:
            h["Authorization"] = f"Bearer {self.state.token}"
        if USE_ODATA_HEADERS:
            if accept_json:
                h["accept"] = "application/json;odata.metadata=minimal;odata.streaming=true"
            if send_json:
                h["Content-Type"] = "application/json;odata.metadata=minimal;odata.streaming=true"
        else:
            if accept_json:
                h["accept"] = "application/json"
            if send_json:
                h["Content-Type"] = "application/json"
        return h

    def _endpoint(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def login(self) -> Tuple[bool, str]:
        if not self.username or not self.password:
            return False, "Faltan usuario/contraseña. Andá a /login."

        url = self._endpoint("/api/v9/login")
        payload = {"userName": self.username, "password": self.password, "clientId": self.client_id}

        r = requests.post(url, headers=self._headers(accept_json=False, send_json=True), json=payload, timeout=30)
        if r.status_code >= 400:
            return False, f"Login falló ({r.status_code}): {r.text[:300]}"

        token = None
        exp_epoch = time.time() + 25 * 60  # fallback 25min

        try:
            data = r.json()
            for key in ("token", "access_token", "jwt", "bearer", "Token", "AccessToken"):
                if isinstance(data, dict) and isinstance(data.get(key), str) and data[key].strip():
                    token = data[key].strip()
                    break
            # best-effort: leer exp del JWT sin verificar firma
            if token and token.count(".") == 2:
                import base64
                payload_b64 = token.split(".")[1] + "=="
                payload_json = base64.urlsafe_b64decode(payload_b64.encode("utf-8")).decode("utf-8", errors="ignore")
                pj = json.loads(payload_json)
                if isinstance(pj, dict) and "exp" in pj:
                    exp_epoch = float(pj["exp"])
        except Exception:
            pass

        if not token:
            return False, "No pude detectar token en la respuesta de login."

        self.state.token = token
        self.state.exp_epoch = exp_epoch
        return True, "OK"

    def ensure_token(self) -> Tuple[bool, str]:
        if self.state.valid():
            return True, "OK"
        return self.login()

    def get_detalle_cuenta(self, cuenta: int) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        ok, msg = self.ensure_token()
        if not ok:
            return False, msg, None

        url = self._endpoint("/api/v9/get-detalle-cuenta")
        payload = {"cuenta": int(cuenta), "timeStamp": 0, "paramPagination": {"pageNumber": 0, "pageSize": 0}}
        r = requests.post(url, headers=self._headers(accept_json=True, send_json=True), json=payload, timeout=30)
        if r.status_code >= 400:
            return False, f"get-detalle-cuenta falló ({r.status_code}): {r.text[:300]}", None

        try:
            return True, "OK", r.json()
        except Exception:
            return False, "Respuesta no es JSON.", None


# ---------------- Aranceles Engine ----------------
REQ_COLS = [
    "Grupo de Arancel",
    "Tipo de Operación (Abreviatura)",
    "Tipo de Operación (Descripción)",
    "Tipo de Especie",
    "Moneda (Símbolo)",
    "Moneda (Descripción)",
    "Arancel (Porcentaje)",
    "Arancel (Mínimo)",
    "Arancel (Cobro en Moneda de la Aplicación)",
    "Está Anulado",
    "Fecha de Vigencia",
]

KEY_COLS = [
    "Grupo de Arancel",
    "Tipo de Operación (Abreviatura)",
    "Tipo de Especie",
    "Moneda (Símbolo)",
]



def _sheet_csv_url() -> str:
    """Devuelve URL CSV export para Google Sheets."""
    if ARANCELES_SHEET_CSV_URL:
        return ARANCELES_SHEET_CSV_URL
    if not ARANCELES_SHEET_ID:
        return ""
    # Nota: funciona si el Sheet está compartido como "Cualquiera con el link: Lector"
    return f"https://docs.google.com/spreadsheets/d/{ARANCELES_SHEET_ID}/export?format=csv&gid={ARANCELES_SHEET_GID}"


def load_aranceles_df() -> pd.DataFrame:
    """Carga aranceles desde Sheets (si está configurado) o desde Excel local."""
    csv_url = _sheet_csv_url()
    if csv_url:
        # Descarga CSV público
        r = requests.get(csv_url, timeout=30)
        r.raise_for_status()
        # pandas lee desde bytes; forzamos utf-8
        from io import BytesIO
        bio = BytesIO(r.content)
        df = pd.read_csv(bio)
        return df
    # fallback Excel
    return pd.read_excel(DEFAULT_EXCEL_PATH)


def load_latest_aranceles(xlsx_path: str) -> pd.DataFrame:
    df = load_aranceles_df()
    missing = [c for c in REQ_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas en Excel: {missing}")

    df = df[REQ_COLS].copy()
    df["Está Anulado"] = pd.to_numeric(df["Está Anulado"], errors="coerce").fillna(0).astype(int)
    df = df[df["Está Anulado"] == 0].copy()

    df["Fecha de Vigencia"] = pd.to_datetime(df["Fecha de Vigencia"], errors="coerce")
    df = df[df["Fecha de Vigencia"].notna()].copy()

    idx = df.groupby(KEY_COLS, dropna=False)["Fecha de Vigencia"].idxmax()
    latest = df.loc[idx].copy()
    latest = latest.sort_values(["Grupo de Arancel", "Tipo de Operación (Descripción)", "Tipo de Especie", "Moneda (Símbolo)"])
    return latest


def list_operaciones(latest: pd.DataFrame, grupo: str) -> List[Tuple[str, str]]:
    sub = latest[latest["Grupo de Arancel"] == grupo]
    pairs = sub[["Tipo de Operación (Abreviatura)", "Tipo de Operación (Descripción)"]].drop_duplicates()
    pairs = pairs.sort_values("Tipo de Operación (Descripción)")
    return [(r["Tipo de Operación (Abreviatura)"], r["Tipo de Operación (Descripción)"]) for _, r in pairs.iterrows()]


def list_especies(latest: pd.DataFrame, grupo: str, op_abbr: str) -> List[str]:
    sub = latest[(latest["Grupo de Arancel"] == grupo) & (latest["Tipo de Operación (Abreviatura)"] == op_abbr)]
    return sorted(sub["Tipo de Especie"].dropna().unique().tolist())



def list_monedas_por_operacion(latest: pd.DataFrame, grupo: str, op_abbr: str) -> List[str]:
    sub = latest[
        (latest["Grupo de Arancel"] == grupo)
        & (latest["Tipo de Operación (Abreviatura)"] == op_abbr)
    ]
    return sorted(sub["Moneda (Símbolo)"].dropna().unique().tolist())


def list_monedas(latest: pd.DataFrame, grupo: str, op_abbr: str, especie: str) -> List[str]:
    sub = latest[
        (latest["Grupo de Arancel"] == grupo)
        & (latest["Tipo de Operación (Abreviatura)"] == op_abbr)
        & (latest["Tipo de Especie"] == especie)
    ]
    return sorted(sub["Moneda (Símbolo)"].dropna().unique().tolist())


def find_arancel(latest: pd.DataFrame, grupo: str, op_abbr: str, especie: str, moneda: str) -> Optional[Dict[str, Any]]:
    sub = latest[
        (latest["Grupo de Arancel"] == grupo)
        & (latest["Tipo de Operación (Abreviatura)"] == op_abbr)
        & (latest["Tipo de Especie"] == especie)
        & (latest["Moneda (Símbolo)"] == moneda)
    ]
    if sub.empty:
        return None
    row = sub.iloc[0].to_dict()
    if isinstance(row.get("Fecha de Vigencia"), pd.Timestamp):
        row["Fecha de Vigencia"] = row["Fecha de Vigencia"].date().isoformat()
    return row

def list_aranceles_por_especie(latest: pd.DataFrame, grupo: str, op_abbr: str, moneda: str) -> List[Dict[str, Any]]:
    """Devuelve todas las especies para (grupo, operación, moneda) con su arancel vigente."""
    sub = latest[
        (latest["Grupo de Arancel"] == grupo)
        & (latest["Tipo de Operación (Abreviatura)"] == op_abbr)
        & (latest["Moneda (Símbolo)"] == moneda)
    ]
    if sub.empty:
        return []
    # ordenar por especie
    sub = sub.sort_values(["Tipo de Especie"])
    out: List[Dict[str, Any]] = []
    for _, r in sub.iterrows():
        row = r.to_dict()
        if isinstance(row.get("Fecha de Vigencia"), pd.Timestamp):
            row["Fecha de Vigencia"] = row["Fecha de Vigencia"].date().isoformat()
        out.append(row)
    return out



# ---------------- Flask App ----------------
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "dev-secret-cambiar")

esco = EscoClient()

try:
    LATEST = load_latest_aranceles(DEFAULT_EXCEL_PATH)  # usa Sheets si está configurado
    EXCEL_OK = True
    EXCEL_ERR = ""
except Exception as e:
    LATEST = pd.DataFrame()
    EXCEL_OK = False
    EXCEL_ERR = str(e)


def require_login():
    # Si no hay sesión, forzar login
    return bool(session.get("esco_logged"))


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    esco.state.token = None
    flash("Sesión cerrada.", "secondary")
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        client_id = request.form.get("client_id", "").strip()

        if not username or not password:
            flash("Usuario y contraseña requeridos.", "danger")
            return render_template("login.html", username=username, client_id=client_id)

        # set creds on client and try login
        esco.set_creds(username, password, client_id)
        esco.state.token = None  # force refresh
        ok, msg = esco.login()
        if not ok:
            flash(msg, "danger")
            return render_template("login.html", username=username, client_id=client_id)

        # store in session (browser cookie) - only while session lives
        session["esco_logged"] = True
        session["ESCO_USERNAME"] = username
        session["ESCO_PASSWORD"] = password
        session["ESCO_CLIENT_ID"] = client_id

        flash("Login exitoso.", "success")
        return redirect(url_for("index"))

    return render_template("login.html", username="", client_id="")


@app.route("/", methods=["GET", "POST"])
def index():
    global LATEST, EXCEL_OK, EXCEL_ERR

    if not require_login():
        return redirect(url_for("login"))

    # Ensure client has current creds (in case server restarted but cookie still there)
    esco.set_creds(session.get("ESCO_USERNAME", ""), session.get("ESCO_PASSWORD", ""), session.get("ESCO_CLIENT_ID", ""))

    cuenta = request.form.get("cuenta", "").strip() if request.method == "POST" else ""
    grupo_desc = ""
    grupo_code = None

    op_abbr = request.form.get("op_abbr", "").strip() if request.method == "POST" else ""
    moneda = request.form.get("moneda", "").strip() if request.method == "POST" else ""

    operaciones = []
    monedas = []
    aranceles_especies = []

    if not EXCEL_OK:
        flash(f"Error cargando Excel '{DEFAULT_EXCEL_PATH}': {EXCEL_ERR}", "danger")
        return render_template(
            "index.html",
            excel_ok=False,
            cuenta=cuenta,
            grupo_desc=grupo_desc,
            grupo_code=grupo_code,
            operaciones=operaciones,
            monedas=monedas,
            op_abbr=op_abbr,
            moneda=moneda,
            aranceles_especies=aranceles_especies,
            excel_path=DEFAULT_EXCEL_PATH,
        )

    if request.method == "POST" and cuenta:
        try:
            cuenta_int = int(cuenta)
        except ValueError:
            flash("El comitente/cuenta debe ser numérico.", "warning")
            return render_template("index.html", excel_ok=True, cuenta=cuenta, excel_path=DEFAULT_EXCEL_PATH)

        ok, msg, data = esco.get_detalle_cuenta(cuenta_int)
        if not ok:
            flash(msg, "danger")
        else:
            grupo_code = None

PRIMARY_KEYS = {
    "codgrupoaroperburs",
    "cod_grupo_ar_oper_burs",
    "grupoaroperburs",
}
FALLBACK_KEYS = {
    "codgrupoaracr",
    "cod_grupo_ar_acr",
    "grupoaracr",
}

def walk(obj):
    nonlocal grupo_code
    if grupo_code is not None:
        return
    if isinstance(obj, dict):
        # 1) prioridad: OperBurs
        for k, v in obj.items():
            if str(k).lower() in PRIMARY_KEYS:
                try:
                    grupo_code = int(v)
                    return
                except Exception:
                    pass
        # seguir recorriendo
        for v in obj.values():
            walk(v)
    elif isinstance(obj, list):
        for it in obj:
            walk(it)

walk(data)

# 2) fallback: ACR (solo si no se encontró OperBurs)
if grupo_code is None:
    def walk_fb(obj):
        nonlocal grupo_code
        if grupo_code is not None:
            return
        if isinstance(obj, dict):
            for k, v in obj.items():
                if str(k).lower() in FALLBACK_KEYS:
                    try:
                        grupo_code = int(v)
                        return
                    except Exception:
                        pass
            for v in obj.values():
                walk_fb(v)
        elif isinstance(obj, list):
            for it in obj:
                walk_fb(it)

    walk_fb(data)

            if grupo_code is None:
                flash("No encontré 'codGrupoArAcr' ni 'codGrupoArOperBurs' en la respuesta de get-detalle-cuenta.", "warning")
            else:
                grupo_desc = GRUPOS_MAP.get(grupo_code, f"Grupo código {grupo_code}")

                if grupo_desc not in LATEST["Grupo de Arancel"].unique().tolist():
                    flash(f"El grupo '{grupo_desc}' no aparece en el Excel (columna 'Grupo de Arancel').", "warning")
                else:
                    operaciones = list_operaciones(LATEST, grupo_desc)
                    if op_abbr:
                        monedas = list_monedas_por_operacion(LATEST, grupo_desc, op_abbr)

                    if op_abbr and moneda:
                        aranceles_especies = list_aranceles_por_especie(LATEST, grupo_desc, op_abbr, moneda)
                        if not aranceles_especies:
                            flash("No encontré aranceles para esa combinación (ya filtrado por vigencia más reciente).", "warning")

    return render_template(
        "index.html",
        excel_ok=True,
        cuenta=cuenta,
        grupo_desc=grupo_desc,
        grupo_code=grupo_code,
        operaciones=operaciones,
        monedas=monedas,
        op_abbr=op_abbr,
        moneda=moneda,
        aranceles_especies=aranceles_especies,
        excel_path=DEFAULT_EXCEL_PATH,
    )


@app.route("/reload-excel", methods=["POST"])
def reload_excel():
    global LATEST, EXCEL_OK, EXCEL_ERR
    try:
        LATEST = load_latest_aranceles(DEFAULT_EXCEL_PATH)  # usa Sheets si está configurado
        EXCEL_OK = True
        EXCEL_ERR = ""
        flash("Aranceles recargados OK (Sheets/Excel).", "success")
    except Exception as e:
        EXCEL_OK = False
        EXCEL_ERR = str(e)
        flash(f"Error recargando aranceles: {EXCEL_ERR}", "danger")
    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
