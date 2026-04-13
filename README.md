[([docs/output.jpb](https://github.com/sparksbenjamin/Vanguarr/blob/main/docs/output.jpg))](https://github.com/sparksbenjamin/Vanguarr/blob/main/docs/output.jpg)

# 🛡️ Vanguarr
### The Scout of the ARR Stack.

[![Docker workflow](https://github.com/sparksbenjamin/Vanguarr/actions/workflows/docker.yml/badge.svg)](https://github.com/sparksbenjamin/Vanguarr/actions/workflows/docker.yml)
[![Tests](https://github.com/sparksbenjamin/Vanguarr/actions/workflows/tests.yml/badge.svg)](https://github.com/sparksbenjamin/Vanguarr/actions/workflows/tests.yml)
[![Latest tag](https://img.shields.io/github/v/tag/sparksbenjamin/Vanguarr?sort=semver)](https://github.com/sparksbenjamin/Vanguarr/tags)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-3776AB?logo=python&logoColor=white)](docs/configuration.md)
[![License: GPL v3](https://img.shields.io/badge/license-GPLv3-blue.svg)](LICENSE)

Current release: `0.2.1`

**Stop scrolling. Start watching.** Vanguarr is a self-hosted media-server recommendation engine that acts as the brain between your watch history and your request stack. It learns what your household actually likes, maps that behavior into durable taste manifests, and scouts for high-confidence media through **Jellyseerr**, **Overseerr**, and other Seer-compatible services.

Instead of a black box, Vanguarr is **explainable AI**. It ranks, scores, and explains every decision, so you stay in control of your server's library instead of hoping a model guessed right.

---

## ✨ Why Vanguarr?

* **🧠 Genuine Intelligence:** Learns from real playback history in **Jellyfin** or **Plex**, not generic "trending" lists.
* **🛠️ Deterministic First:** Decisions are scored in code first. AI/LLMs help with discovery and a final vote, but they do not own the pipeline.
* **👤 Persistent User Profiles:** Every user gets a durable JSON taste manifest you can inspect, edit, and tune.
* **🔌 Flexible AI Support:** Run local with **Ollama** or connect to hosted providers like **Claude** and **GPT**.
* **🎬 Native Jellyfin Experience:** Vanguarr can surface personalized `Suggested Movies` and `Suggested Shows` directly inside Jellyfin with the companion plugin.
* **🛡️ Operator-Led:** The dashboard, manifest editor, and War Room log make it clear why something was suggested or requested.

## 🎯 What Vanguarr Does

Vanguarr sits between the media server your users watch and the request stack that grows your library.

* It watches what people actually play.
* It turns that behavior into durable user taste profiles.
* It ranks both current-library suggestions and requestable content.
* It explains why each recommendation earned its spot.
* It can surface those picks natively inside Jellyfin.

In plain English: Vanguarr watches, learns, scouts, scores, and reports back before anything gets added to the stack.

## 🚀 Quick Start

1. Pick the deployment path that fits your stack.
2. Add your media server, Seer-compatible service, and optional LLM credentials.
3. Start Vanguarr.
4. Open the dashboard and run `Profile Architect`.
5. Run `Decision Engine` to score candidates immediately.

Then open:

```text
http://localhost:8000
```

Pick the path that matches how you run the rest of your stack:

* `Docker Compose` is the fastest default for most self-hosted setups.
* `OKD` is the right fit if you already run your apps on OpenShift or Kubernetes.
* `Unraid` is the easiest path if your media stack already lives in Unraid and you want persistent appdata storage.

After Vanguarr is up, finish the real setup from the web UI in `/settings`:

* connect Jellyfin or Plex
* connect Jellyseerr, Overseerr, or another Seer-compatible service
* add TMDb and LLM providers if you want enrichment and blended AI scoring

If you want to build and run Vanguarr directly from this repo instead of using the published container image, use the manual path below.

<details>
<summary><strong>Manual Repo Deployment</strong></summary>

```bash
docker compose up -d --build
```

If you want to seed first-boot values from environment variables, copy [`.env.example`](.env.example) to `.env` before you start the stack. Otherwise, Vanguarr will boot on its built-in defaults and you can finish setup from the UI.

</details>

<details>
<summary><strong>Docker Compose Example</strong></summary>

```yaml
services:
  vanguarr:
    image: ghcr.io/sparksbenjamin/vanguarr:latest
    container_name: vanguarr
    restart: unless-stopped
    ports:
      - "8000:8000"
    volumes:
      - ./data:/data
```

Then open `http://localhost:8000` and finish the integrations in `/settings`.

</details>

<details>
<summary><strong>OKD Example</strong></summary>

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: vanguarr-data
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 5Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: vanguarr
spec:
  replicas: 1
  selector:
    matchLabels:
      app: vanguarr
  template:
    metadata:
      labels:
        app: vanguarr
    spec:
      containers:
        - name: vanguarr
          image: ghcr.io/sparksbenjamin/vanguarr:latest
          imagePullPolicy: Always
          ports:
            - name: http
              containerPort: 8000
              protocol: TCP
          readinessProbe:
            httpGet:
              path: /healthz
              port: http
            initialDelaySeconds: 10
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /healthz
              port: http
            initialDelaySeconds: 30
            periodSeconds: 20
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: vanguarr-data
---
apiVersion: v1
kind: Service
metadata:
  name: vanguarr
spec:
  selector:
    app: vanguarr
  ports:
    - name: http
      port: 8000
      targetPort: http
      protocol: TCP
---
apiVersion: route.openshift.io/v1
kind: Route
metadata:
  name: vanguarr
spec:
  to:
    kind: Service
    name: vanguarr
  port:
    targetPort: http
```

Once the route is live, open Vanguarr and configure your integrations from `/settings`. If your cluster terminates TLS at the Route layer, you can add a `tls` block here without changing the app container.

</details>

<details>
<summary><strong>Unraid Example</strong></summary>

```yaml
services:
  vanguarr:
    image: ghcr.io/sparksbenjamin/vanguarr:latest
    container_name: vanguarr
    restart: unless-stopped
    network_mode: bridge
    ports:
      - "8000:8000"
    extra_hosts:
      - "host.docker.internal:host-gateway"
    volumes:
      - /mnt/user/appdata/vanguarr:/data
```

If Ollama is running on the Unraid host, you can still use `host.docker.internal` later from the Vanguarr settings UI.

</details>

From there:

* `Profile Architect` builds user taste manifests from real watch history.
* `Decision Engine` scouts requestable media and scores it.
* `Suggested For You` and `Library Sync` keep Jellyfin recommendations fresh.

If you want the Jellyfin-side experience too, follow the plugin guide in [docs/jellyfin-plugin.md](docs/jellyfin-plugin.md).

## 🎥 The Experience

With Vanguarr in place, your stack can feel a lot less like "manually hunting for something to watch" and a lot more like:

* Users open Jellyfin and browse personalized `Suggested Movies` and `Suggested Shows`.
* Vanguarr keeps learning from what they actually finish, repeat, and ignore.
* Your request stack gets fed with higher-confidence additions instead of random noise.
* You can still inspect and override the logic when you want to.

## 🍿 Jellyfin Plugin

### Built To Feel Native
Vanguarr is not just a background service. It also ships with a companion Jellyfin plugin that adds personalized `Suggested Movies` and `Suggested Shows` views directly inside Jellyfin.

* **No Symlinks:** No duplicate libraries or filesystem hacks.
* **User-Specific:** Every family member gets their own curated suggestions.
* **Native Playback:** Suggestions resolve back to real Jellyfin items, so browsing and playback stay seamless.
* **Operator-Friendly:** Vanguarr keeps the scoring brain, while Jellyfin stays the playback surface.

👉 [View Plugin Overview](jellyfin-plugin/README.md)  
👉 [View Plugin Setup Guide](docs/jellyfin-plugin.md)

## 📚 Documentation

Use the front page for the pitch. Use the docs below for the technical detail.

* [Jellyfin Plugin Setup](docs/jellyfin-plugin.md)
* [Jellyfin Plugin Overview](jellyfin-plugin/README.md)
* [Release Notes](CHANGELOG.md)
* [Configuration Reference](docs/configuration.md)
* [How Vanguarr Works](docs/how-it-works.md)

<details>
<summary><strong>Need the technical setup path?</strong></summary>

### Fast Setup Checklist

* Configure Jellyfin or Plex as the media source.
* Configure Jellyseerr, Overseerr, or another Seer-compatible service.
* Add optional TMDb and LLM settings if you want richer enrichment.
* Start with Docker Compose.
* Open `/settings` and finish any runtime tuning in the UI.

### Helpful Starting Points

* [`.env.example`](.env.example) for first-run values
* [`docker-compose.yml`](docker-compose.yml) for the default container layout
* [docs/configuration.md](docs/configuration.md) for runtime settings, schedules, and deployment notes

</details>

<details>
<summary><strong>Want the deeper technical breakdown?</strong></summary>

Vanguarr runs a few cooperating systems:

* `Profile Architect` builds durable user taste manifests from watch history.
* `Decision Engine` scouts, enriches, ranks, and filters requestable content.
* `Library Sync` indexes available Jellyfin media so in-library suggestions stay honest.
* The Jellyfin companion plugin resolves those suggestions back to native playable library items.

The full architecture, data flow, and profile model live in [docs/how-it-works.md](docs/how-it-works.md).

</details>

<details>
<summary><strong>Running locally or developing on the repo?</strong></summary>

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

On Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

Run tests with:

```bash
python -m pytest
```

</details>

## 🧭 Why It Matters

Most media automation tools can request content.

Very few can explain why that content belongs in *your* library for *your* users.

That is the gap Vanguarr is built to fill.

## License

This project is licensed under the GNU GPL v3. See [LICENSE](LICENSE).
