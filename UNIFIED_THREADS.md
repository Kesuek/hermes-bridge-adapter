# Unified Threads — eine Agent-Session über mehrere Bridges

> **Das Alleinstellungsmerkmal des Bridge Adapters.** Weil jede Bridge über denselben JSON-Datei-Vertrag an den Adapter angebunden ist, können mehrere Bridges eine **gemeinsame Agent-Session** teilen. Native Gateway-Adapter (Telegram, Discord, Matrix, …) sind voneinander isoliert — jede Plattform hat ihre eigene Session. Der Bridge Adapter macht daraus **eine Konversation über alle deine Messaging-Welten**.

## Konzept

Ein **Unified Thread** mappt alle Member einer Gruppe auf denselben virtuellen Thread:

```
chat_type = "thread"
chat_id   = "unified"
thread_id = <name>
```

Der Gateway baut daraus den Session-Key `agent:main:bridge_adapter:thread:unified:<name>` — **ohne user_id** (verifiziert gegen `gateway/session.py`: bei `thread` + `thread_sessions_per_user=False` wird kein user_id angehängt). Dadurch teilen sich alle Member **eine** Session, und der Agent prefixt jede Nachricht automatisch mit `[Name]`.

**Kein Core-Eingriff** — die Session-Mechanik ist im Gateway bereits vorhanden, der Adapter mappt nur darauf.

## `/unified`-Befehle

Eine Nachricht, die mit `/unified` beginnt, wird vom Adapter als Befehl geparst (erreicht den Agenten nie). Befehle kommen aus jeder Member-Bridge wie eine normale Nachricht:

| Befehl | Beschreibung |
|--------|--------------|
| `/unified create <name>` | Thread anlegen; der Sender wird erster Member (und **Leader**) |
| `/unified status` | Alle Threads + Memberzahl + Modus auflisten |
| `/unified join <name>` | Bestehendem Thread beitreten |
| `/unified leave <name>` | Thread verlassen |
| `/unified members <name>` | Member eines Threads auflisten |
| `/unified mode <name> <mode>` | Modus setzen — `participant` / `reactive` / `off` / `silent` / `protokoll` |
| `/unified protokoll open <name> [sitzung]` | Protokoll-Sitzung öffnen (Leader-only) |
| `/unified protokoll close <name>` | Protokoll-Sitzung schließen (Leader-only) |
| `/unified help` | Befehlsliste anzeigen |

## Adressierung

Ein Member wird automatisch auf den virtuellen Thread gemappt — seine normalen Nachrichten laufen in die gemeinsame Session. Der Agent (oder ein Cron-Job) antwortet mit dem speziellen Ziel:

```
unified~<name>
```

`send()` special-cased das `unified~`-Prefix **vor** der Bridge-Auflösung (`unified` ist kein registrierter Bridge-Prefix) und schreibt **eine Outbox-JSON pro Member** in dessen eigenes `outbox/<bridge>/`:

```
outbox/imsg/<uuid>.json   → target = imsg~<chat_id>
outbox/talk/<uuid>.json   → target = talk~<chat_id>
```

Jeder Wrapper liefert seine Kopie über seine eigene Plattform-API — **eine Agent-Antwort erreicht alle Bridges im Thread**.

> **⚠️ Session chat_id muss routable sein.** Die Session `chat_id` ist `unified~<name>`, NICHT das nackte `unified`. Wenn der Agent *über die Session* antwortet (nicht explizit `unified~<name>` adressiert), sendet der Gateway an die Session chat_id. Ist das `unified`, findet `_resolve_bridge_or_none` kein `~`-Prefix → `bridge prefix unknown`. `unified~<name>` triggert den Multicast-Branch. (Regression: live entdeckt 2026-08-10, Commit `dcd959c`.)

## Teilnehmer-Modi

Jeder Thread hat ein `mode`-Feld (Default `participant`), das steuert, wie der Adapter eingehende Member-Nachrichten dispatchen. `reactive`/`off`/`silent`/`protokoll` werden **deterministisch vor dem Gateway** durchgesetzt — nur `participant` lässt den Agenten entscheiden.

| Modus | Verhalten | Setzen mit |
|-------|-----------|------------|
| `participant` (Default) | Agent entscheidet, ob er antwortet. Via `platform_hint` gelehrt, `NO_REPLY` zu emittieren, wenn er nichts beizutragen hat; der Gateway unterdrückt diese Antwort. | `/unified mode <name> participant` |
| `reactive` | Mention-Gating wie Gruppenchat: nur Nachrichten, die den Agenten erwähnen (`@hermes` oder `mention_patterns`-Match), werden dispatched. Un-erwähnte werden gedroppt + Inbox-Datei gelöscht. | `/unified mode <name> reactive` |
| `off` | Agent bekommt gar nichts — kein Kontext, kein Turn. Jede Nachricht wird gedroppt + Inbox-Datei gelöscht. | `/unified mode <name> off` |
| `silent` | Mute-Schalter: Agent liest mit, antwortet nie. Jede Nachricht wird gebuffert und periodisch (`digest_interval`) als **ein** Turn geflusht, markiert `[Silent digest — read only, do not reply]`. | `/unified mode <name> silent` |
| `protokoll` | Protokoll-Modus: eingehende Nachrichten werden in die laufende Sitzung gesammelt statt dispatched. Agent antwortet nicht, solange eine Sitzung offen ist. | `/unified protokoll open <name>` |

> **Hinweis:** `reactive`/`off` droppen die Nachricht im Adapter — der Agent sieht sie in diesem Turn nie. `silent` buffert sie in einen Digest (Agent liest mit, antwortet nie). `protokoll` sammelt sie in die Sitzung. Die gemeinsame Session akkumuliert trotzdem Historie aus den Turns, die der Agent *sieht*.

### Leader-Markierung

Der Thread-Ersteller (`created_by`) ist der **Leader**. Die Routing-Zeile, die der Adapter an jede Unified-Thread-Nachricht hängt, markiert den Leader explizit:

```
Message from ronny, bridge imsg, unified thread 'projekt' (2 members), reply to unified~projekt [Ronny Leader]
```

Nicht-Leader-Nachrichten tragen dieselbe Zeile ohne `[<Name> Leader]`-Suffix.

### Protokoll-Lifecycle

`protokoll` ist ein Leader-only-Lifecycle, um den Verlauf eines Threads als Artefakt festzuhalten (z.B. Besprechungsprotokoll):

1. **Open** — der Leader führt `/unified protokoll open <name> [sitzung]` aus. Der Adapter legt einen Live-`protokoll`-State auf dem Thread an (`name`, `opened_at`, `messages: []`) und setzt den Modus auf `protokoll`. Ab jetzt werden eingehende Nachrichten in `protokoll.messages` **gesammelt** statt dispatched — der Agent antwortet nicht.
2. **Close** — der Leader führt `/unified protokoll close <name>` aus. Der Adapter rendert die gesammelten Nachrichten als Markdown nach `<bridge_dir>/protokoll/<name>/<sitzung>.md`, löscht den Live-`protokoll`-State und setzt den Modus zurück auf `participant`.
3. **Retroaktiv** — eine Sitzung, die keine Nachrichten sammelte, erzeugt ein Platzhalter-Artefakt; der Agent kann auf Anforderung die bestehende Thread-Historie zusammenfassen.

Nur der Leader (`created_by`) darf `open`/`close`. Nicht-Leader-Versuche werden mit klarer Meldung abgelehnt. Der Sitzungsname defaultet auf den Thread-Namen.

## Adaptive Zustandsmaschine

Jeder Thread trägt eine Zustandsmaschine, die das Dispatch-Verhalten an die Nachrichtenfrequenz anpasst:

```
idle → active → digesting
```

- **`idle`** — noch keine Nachrichten (Initialzustand).
- **`active`** — Nachrichten gesehen; jede wird als eigener Turn dispatched (normales `participant`-Verhalten).
- **`digesting`** — hohe Frequenz (3 in 30s oder 5 in 60s, Sliding Window). Eingehende Nachrichten werden **gebuffert** statt dispatched. Nach `digest_interval` (60s) wird der Buffer als **ein** Turn geflusht: ein `MessageEvent`, dessen Text ein `[System: N messages from M users]`-Header + eine `[HH:MM] [sender] text`-Zeile pro gebufferter Nachricht ist. Danach zurück zu `active` mit kurzem Cooldown.

State + Buffer persistieren in `unified_threads.json` (der `_adaptive`-Block) — ein Gateway-Restart verliert das in-flight Digest-Fenster nicht. Adaptive greift nur im **`participant`**-Modus. `silent` nutzt denselben Buffer, sammelt aber **immer** (Mute-Schalter), nicht nur bei hoher Frequenz.

Die Schwellwerte sind Klassen-Konstanten auf `BridgeAdapter` (`ADAPTIVE_THRESHOLD_30`, `ADAPTIVE_THRESHOLD_60`, `ADAPTIVE_DIGEST_INTERVAL`, `ADAPTIVE_COOLDOWN`).

## Reply-To-Ketten über Bridges

Eine Reply-Kette auf einer einzelnen Bridge nutzt die bridge-lokale Message-ID (`reply_to`). Über Bridges ist diese ID bedeutungslos — der iMessage-Wrapper kennt die Talk-Message-ID nicht. Der Adapter überbrückt das mit einer persistierten Map:

```
<bridge_dir>/reply_map.json
{ "<gateway_msg_id>": {"bridge": "imsg", "local_msg_id": "msg_abc"}, ... }
```

- **Inbound** — beim Dispatch zeichnet der Adapter `gateway_msg_id → {bridge, local_msg_id}` auf. Die Gateway-ID ist die `message_id` des Events; die lokale ID ist die `id`/`message_id` der Inbox-JSON. Ohne Gateway-ID fällt er auf eine UUID zurück.
- **Outbound** — `send()`/`send_image()`/`send_document()` (und der `unified~`-Multicast-Pfad) lösen ein `reply_to`, das einer Gateway-ID in der Map entspricht, zur gespeicherten `local_msg_id` auf. Eine bridge-lokale `reply_to` passiert unverändert.

Die Datei wird bei `connect()` geladen und bei jeder Inbound-Registrierung neu geschrieben — Reply-Ketten überleben einen Restart.

## Member-Deduplizierung

Dieselbe Person kann auf zwei Bridges unter verschiedenen Aliases erscheinen — `ronny.pietschke@icloud.com` auf iMessage und `ronny` auf Talk — aber es ist eine Person und sollte ein Member eines Unified Threads sein. Der Adapter führt Aliase über eine persistierte Identitäts-Map zusammen:

```
<bridge_dir>/identity_map.json
{ "ronny": ["ronny.pietschke@icloud.com", "+491****4968", "ronny"] }
```

- **`_resolve_identity(user_id)`** mappt eine bridge-lokale user_id auf die kanonische Person (gibt die user_id selbst zurück, wenn unbekannt).
- **Member-Record** — jeder Member bekommt ein `person`-Feld (die kanonische Identität) und ein `addresses`-Array aus `{bridge, chat_id, user_id}`-Einträgen.
- **Join-Dedup** — `_cmd_unified_join` prüft, ob die kanonische `person` des Senders bereits Member ist. Wenn ja, wird die neue `{bridge}:{chat_id}`-Adresse in das `addresses`-Array des bestehenden Members gemerged statt einen doppelten Member-Eintrag zu erzeugen. Der primäre Member-Key bleibt die erste Adresse, von der die Person beigetreten ist.
- **Inbound-Routing** — `_find_unified_for_member` scannt sowohl die Top-Level-`{bridge}:{chat_id}`-Keys als auch die gemerged `addresses`-Arrays, sodass eine Nachricht von jeder Bridge einer Person zum gemeinsamen Thread mappt.
- **Multicast** — `send("unified~<name>")` multicastet an die primäre Adresse **und** jede gemerged Adresse (deduped), sodass eine Person auf zwei Bridges die Antwort auf beiden erhält.

Die Datei wird bei `connect()` geladen. Unbekannte Aliase passieren unverändert — die Identitäts-Map ist rein opt-in.

## Persistenz

Unified Threads werden in `<bridge_dir>/unified_threads.json` persistiert:

```json
{
  "projekt": {
    "name": "projekt",
    "created_at": "2026-08-10T10:00:00+02:00",
    "created_by": "ronny",
    "members": {
      "imsg:u1": {
        "bridge": "imsg", "chat_id": "u1", "user_id": "ronny.pietschke@icloud.com",
        "user_name": "ronny.pietschke@icloud.com", "person": "ronny",
        "joined_at": "...",
        "addresses": [
          {"bridge": "imsg", "chat_id": "u1", "user_id": "ronny.pietschke@icloud.com"},
          {"bridge": "talk", "chat_id": "t1", "user_id": "ronny"}
        ]
      }
    },
    "aliases": [],
    "mode": "participant",
    "_adaptive": {"state": "idle", "buffer": [], "last_msg_ts": 0.0, "digest_until": 0.0, "cooldown_until": 0.0},
    "protokoll": null
  }
}
```

Member sind nach `{bridge}:{chat_id}` keyed (die erste Adresse, von der eine Person beigetreten ist). Die Datei wird bei `connect()` geladen und bei jedem mutierenden Befehl neu geschrieben — Threads überleben einen Gateway-Restart. Während eine Protokoll-Sitzung offen ist, hält `protokoll` `{name, opened_at, opened_by, messages: [...]}`; nach `close` revertiert es auf `null`. Der `_adaptive`-Block (T-061) trackt den Zustandsmaschinen-State + Message-Buffer.

## Notes / Limits

- **Auth bleibt Framework-seitig.** Ein auf einer Bridge nicht autorisierter User wird vom Gateway-Authz-Mixin gedroppt, bevor das Adapter-Mapping ihn sieht — er kann keinem Thread beitreten.
- **Mention-Patterns** treiben den `reactive`-Modus (und das Gruppenchat-Gating). Die Default-Patterns matchen `@hermes` / `hermes agent`; eine Bridge kann via `mention_patterns` in ihrem Manifest / `BRIDGE_MENTION_PATTERNS` übersteuern.
- **`NO_REPLY`-Marker** ist nur im `participant`-Modus relevant — der Agent emittiert das Literal `NO_REPLY` (oder `[SILENT]`) und der Gateway unterdrückt die Zustellung. Die anderen Modi droppen oder buffern deterministisch im Adapter, bevor der Agent je aufgerufen wird.
- **Adaptive + Modi** — adaptive Bündelung greift nur im `participant`-Modus; `reactive`/`off` droppen un-erwähnte Nachrichten ohnehin (kein Digest nötig), `protokoll` sammelt in die Sitzung, `silent` buffert immer (Mute-Schalter).
- **Identitäts-Map ist opt-in** — ohne `identity_map.json`-Eintrag ist die `person` eines Senders gleich seiner rohen `user_id`, sodass zwei verschiedene Leute mit derselben ID auf verschiedenen Bridges gemerged würden. Explizite Einträge kontrollieren, welche Aliase zusammengehören.
