# Vejas — spec de la démo minimale (v0)

**But unique : tester la traction avant de construire.** Budget dur : 2 week-ends (lane FEU — la lane revenu, relances IT + 31/10, reste intouchée). Tout ce qui n'est pas nécessaire à la démo enregistrée est hors périmètre.

## Le test de distribution (la seule métrique qui compte)

Publier ensemble, la même semaine : le manifeste (blog perso, puis HN « Show HN » avec le repo, lobste.rs, r/rust, communauté NATS Slack/Discord, X). Un manifeste sans artefact prend des commentaires « vaporware » ; l'essai porte le récit, le repo porte la preuve.

**Seuils à 4 semaines** (décidés maintenant, à froid) :
- **GO approfondir** : ≥ 300 étoiles OU ≥ 5 conversations entrantes sérieuses (« on a exactement ce problème », issue détaillée, DM d'un opérateur).
- **Shelve sans regret** : en dessous. Coût total : 2 week-ends. Le repo reste, git n'expire pas.

## Périmètre v0

### Runtime (Rust, quelques centaines de lignes — pas plus)
- Lit `flows/*.py` (manifest implicite : un fichier = un flow).
- Lance chaque flow en **subprocess** avec l'env NATS injecté ; supervise, restart avec backoff.
- Consommateurs JetStream pull → at-least-once ; ack/nack géré par le SDK.
- Traces : OTLP export (ou logs structurés JSON v0 + une page HTML statique qui les affiche — au plus simple).
- Un endpoint `/healthz` et un `/topology` (JSON : flows, subjects, états).

### SDK Python (`vejas-sdk`, ~150 lignes)
```python
from vejas import flow, emit

@flow(source="vx.stripe.events")
def stripe_alerts(event):
    if event["amount"] > 50000:
        emit("vx.slack.out", {"text": f"💰 {event['amount']/100}€ — {event['customer']}"})
```
- `@flow(source=...)` : abonnement JetStream, ack auto en sortie de fonction, nack+retry sur exception.
- `emit(subject, payload)` : publication. Rien d'autre en v0.

### Connecteurs (exactement 2 — des services NATS ordinaires)
1. **http-in** : webhook HTTP → publie sur `{prefix}.events` (signature Stripe vérifiée en option, sinon passthrough).
2. **slack-out** : consomme `vx.slack.out` → webhook Slack entrant.

La « convention de subjects » qui fait office d'interface plug-in est documentée en 20 lignes dans le README : c'est l'argument central du manifeste, il doit être visible.

### Serveur MCP (TypeScript rapide, sans honte)
Outils : `list_connectors`, `scaffold_flow(name, source)`, `run_flow_test(name, fixture)`, `deploy_flow(name)` (= commit + signal reload), `read_traces(flow, n)`.

### Livraison
- `docker-compose.yml` : 2 services (nats + vejas). C'est l'argument « ultra-light » — il doit être vrai.
- Licence Apache-2.0. Aucun format propriétaire nulle part.

## Le script de la démo enregistrée (asciinema/GIF, < 3 min)

1. `docker compose up` (2 conteneurs démarrent, rien d'autre).
2. Dans Claude Code : « Crée un flux qui reçoit les webhooks Stripe et poste un résumé Slack quand le montant dépasse 500 €. Teste-le avec la fixture, puis déploie. »
3. L'agent (via MCP) : liste les connecteurs → scaffolde `flows/stripe_alerts.py` → le fichier est du Python nu, lisible à l'écran → lance le test sur fixture → déploie.
4. `curl` d'un événement Stripe factice → le message tombe dans Slack → la trace apparaît dans la page monitoring.
5. Plan final : l'arborescence git. Aucune UI de construction n'a été ouverte.

## Non-goals v0 (verrouillés)
Pas d'auth, pas de multi-tenant, pas de claims de perf (au plus un script de bench honnête), pas de catalogue de connecteurs, pas d'UI de monitoring riche (une page), pas de scheduler/cron, pas de DAG multi-étapes (un flow = source → code → emit, point). Chaque « et si on ajoutait » va dans `IDEAS.md`, relu seulement si le seuil GO est franchi.

## Week-end 1 vs week-end 2
- **WE1** : SDK + runtime + http-in + slack-out + compose. Fin de WE1 : le flux Stripe→Slack tourne, écrit à la main.
- **WE2** : serveur MCP + page monitoring + enregistrement de la démo + relecture du manifeste + Show HN prêt à partir (titre : « Show HN: Vejas – an integration platform with no builder UI, flows are written by your agent »).

## Addendum (18/08) — mappings : la main au non-dev

Objection de Cyril intégrée au périmètre : le mapping était le seul écran que les non-devs touchaient. Convention actée : `MAPPING = {...}` **littéral** dans le flow (chemins pointés + transforms nommées, registre dans `vejas/mapping.py`) → extractible par AST (`python -m vejas mappings flows/`), rendable en table 2 colonnes avec échantillons réels, corrigeable par édition contrainte du littéral OU instruction à l'agent → patch git montré en avant/après sur échantillons. Le non-dev valide du comportement ; l'artefact reste du code. Implémenté en v0 (SDK + CLI + flow d'exemple) ; la vue HTML = WE2+.

## Addendum 2 (18/08) — v0 construite + amendements de vision

**v0 opérationnelle** (smoke test complet passé) : runtime Rust (supervision + mtime-restart + /healthz /topology /surface /panel /surface/set /reload), SDK Python (@flow/emit, at-least-once, surface/set par AST), connecteurs http-in + slack-out, flow stripe_alerts, panel HTML embarqué. Boucle de correction prouvée : alerte à seuil 500 → correction métier à 2000 via API → alerte supprimée → retour 500 → alerte revient.

**Amendements actés (revue de vision du 18/08) :**
- **A. Correction par l'exemple** : les littéraux couvrent chemins/renommages/constantes/tables d'énumération, PAS plus (refuser le DSL de transforms qui recréerait du low-code). Au-delà : l'expert corrige des **sorties attendues sur événements réels** (golden tests) ; l'agent compile les exemples en code ; les tests sont le contrat.
- **B. Blast radius** : proposer → **shadow-replay** des N derniers événements réels (JetStream le rend trivial) → diff avant/après → approuver → promouvoir. + rollback 1 clic + journal d'audit. C'est ce qui rend « n'importe quel expert métier » vrai en production.
- **C. Monétisation** : runtime + SDK = OSS gratuit (wedge dev/HN) ; **le panel métier = le produit payant** (collaboration, approbations, audit, SSO). Le budget est côté métier.
- Borne : l'IA est supérieure pour *écrire* l'algorithmique, pas encore pour *opérer* sans surveillance — les incidents passent par la même boucle proposer→approuver (l'agent diagnostique sur traces, propose le patch, l'humain approuve).
