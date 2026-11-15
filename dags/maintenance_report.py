"""
Partie 4 - Rapport de maintenance, genere par la derniere tache du DAG.

Lit l'etat de la flotte dans PostgreSQL (copie de la zone Gold) et ecrit dans
reports/ :
  rapport_maintenance_<date>.md      rapport lisible (resume, modele, moteurs a traiter)
  moteurs_a_surveiller_<date>.csv    liste a transmettre a l'equipe de maintenance
"""
import csv
import logging
import os
from datetime import datetime

log = logging.getLogger(__name__)

PG = {"host": "postgres", "port": 5432, "dbname": "maintenance",
      "user": "maintenance", "password": "maintenance"}
REPORTS_DIR = "/opt/airflow/reports"


def action(row: dict) -> str:
    if row["etat_predit"] == "Critique":
        return "Arret et maintenance immediate"
    if row["etat_predit"] == "A surveiller":
        return f"Planifier la maintenance sous {max(round(row['rul_predite']), 0)} cycles"
    return "Aucune action"


def fetch(conn_params: dict) -> tuple:
    import psycopg2
    with psycopg2.connect(**conn_params) as conn, conn.cursor() as cur:
        cur.execute("""SELECT engine_id, dernier_cycle, rul_predite, etat_predit, proba_critique,
                              rul_reelle, model_version, date_calcul
                       FROM flotte_sante ORDER BY priorite, rul_predite""")
        cols = [d[0] for d in cur.description]
        flotte = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.execute("SELECT * FROM model_metrics ORDER BY version DESC LIMIT 1")
        cols = [d[0] for d in cur.description]
        r = cur.fetchone()
        metrics = dict(zip(cols, r)) if r else {}
    return flotte, metrics


def build_markdown(flotte: list, metrics: dict, date: str, run_id: str = "") -> str:
    counts = {e: sum(1 for r in flotte if r["etat_predit"] == e) for e in ("Critique", "A surveiller", "Sain")}
    todo = [r for r in flotte if r["etat_predit"] != "Sain"]
    lines = [
        f"# Rapport de maintenance - {date}",
        "",
        f"Genere le {datetime.now():%Y-%m-%d %H:%M} par le DAG Airflow `retrain_and_report`"
        + (f" (run `{run_id}`)" if run_id else "") + ".",
        "",
        "## Resume de la flotte",
        "",
        "| Etat | Moteurs |",
        "| --- | --- |",
        f"| Critique (RUL <= 15 cycles) | {counts['Critique']} |",
        f"| A surveiller (15 < RUL <= 50) | {counts['A surveiller']} |",
        f"| Sain (RUL > 50) | {counts['Sain']} |",
        f"| **Total** | **{len(flotte)}** |",
        "",
        "## Modele utilise",
        "",
    ]
    if metrics:
        lines += [
            f"- Version : `{metrics.get('version')}` (entraine le {metrics.get('date_entrainement')})",
            f"- RUL (GBTRegressor) : RMSE {metrics.get('test_rmse')} cycles, MAE {metrics.get('test_mae')} cycles",
            f"- Etat (RandomForest) : F1 {metrics.get('test_f1')}, rappel de la classe Critique {metrics.get('test_recall_critique')}",
        ]
    else:
        lines.append("- Aucune metrique disponible.")
    lines += ["", f"## Moteurs a traiter ({len(todo)})", ""]
    if todo:
        lines += ["| Moteur | Cycle actuel | RUL predite | Etat | Proba. critique | Action |",
                  "| --- | --- | --- | --- | --- | --- |"]
        for r in todo:
            lines.append(f"| {r['engine_id']} | {r['dernier_cycle']} | {r['rul_predite']:.0f} | "
                         f"{r['etat_predit']} | {r['proba_critique']:.0%} | {action(r)} |")
    else:
        lines.append("Aucun moteur a traiter.")
    return "\n".join(lines) + "\n"


def write_csv(todo: list, path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["moteur", "cycle_actuel", "rul_predite", "etat", "proba_critique", "action"])
        for r in todo:
            w.writerow([r["engine_id"], r["dernier_cycle"], round(r["rul_predite"], 1),
                        r["etat_predit"], round(r["proba_critique"], 3), action(r)])


def generer_rapport(date: str = None, run_id: str = "", reports_dir: str = REPORTS_DIR,
                    conn_params: dict = PG) -> dict:
    """Point d'entree appele par le PythonOperator du DAG."""
    date = date or datetime.now().strftime("%Y-%m-%d")
    flotte, metrics = fetch(conn_params)
    if not flotte:
        raise ValueError("La table flotte_sante est vide : lancer d'abord l'export PostgreSQL")

    os.makedirs(reports_dir, exist_ok=True)
    md_path = os.path.join(reports_dir, f"rapport_maintenance_{date}.md")
    csv_path = os.path.join(reports_dir, f"moteurs_a_surveiller_{date}.csv")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(build_markdown(flotte, metrics, date, run_id))
    todo = [r for r in flotte if r["etat_predit"] != "Sain"]
    write_csv(todo, csv_path)

    critiques = [r["engine_id"] for r in flotte if r["etat_predit"] == "Critique"]
    log.info("Rapport ecrit : %s (%d moteurs a traiter)", md_path, len(todo))
    if critiques:
        log.warning("ALERTE : %d moteur(s) critique(s) : %s", len(critiques), critiques)
    return {"rapport": md_path, "csv": csv_path, "moteurs_a_traiter": len(todo), "critiques": len(critiques)}
