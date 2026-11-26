# AtlasFarm Web Console

Vite + React + TypeScript + Tailwind operations console for the AtlasFarm control plane.

## Run locally

Start the control plane on `http://localhost:8080`, then run:

```powershell
npm install
npm run dev
```

Open `http://localhost:5173`. The console polls worker, job, and rollout APIs every
five seconds. The API base can be changed with `VITE_API_BASE_URL` (the default is
`http://localhost:8080`).

Run `npm run build` to type-check and create the production bundle in `dist/`.
