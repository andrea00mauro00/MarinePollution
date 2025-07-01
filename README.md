# MarinePollution

## Getting Started

1. Clone this repo  
   \`\`\`bash
   git clone <REMOTE_URL>
   cd MarinePollution
   \`\`\`
2. Copia l’esempio di ambiente e modificalo  
   \`\`\`bash
   cp .env.example .env
   # Inserisci API keys, credenziali, ecc.
   \`\`\`
3. (Opzionale) Crea e attiva il virtualenv  
   \`\`\`bash
   python3 -m venv .venv-alerts
   source .venv-alerts/bin/activate
   pip install -r requirements.txt
   \`\`\`
4. Avvia i container  
   \`\`\`bash
   docker-compose up -d
   \`\`\`
5. Verifica i log dei producer  
   \`\`\`bash
   docker-compose logs -f water-metrics-producer copernicus-buoy-producer
   \`\`\`

## Struttura

- \`ingestion/\`: script Python + Dockerfile per i producer  
- \`docker-compose*.yml\`: definizione dei servizi
- \`consumer_alerts.py\`, \`main.py\`, ecc.

## Testing

Per eseguire i test unitari installa prima `pytest` e poi lancia:
```bash
pip install pytest
pytest
```
