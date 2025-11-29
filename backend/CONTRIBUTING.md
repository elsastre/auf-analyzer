# Contributing

## Flujo de ramas
- **main**: estable, protegido. Solo merge desde PR.
- **develop**: integración continua de features.
- Features: eat/..., fixes: ix/..., chores: chore/....

## Cómo contribuir (paso a paso)
1. Crear rama desde \develop\:
   \git checkout develop && git pull && git checkout -b feat/mi-cambio\
2. Instalar en modo editable y deps:
   \python -m venv .venv && .\.venv\Scripts\Activate.ps1 && pip install -e .\
3. Correr tests locales:
   \pip install pytest && pytest -q\
4. Commit con prefijos convencionales (feat, fix, docs, chore, ci, refactor).
5. Push y abrir PR → **develop**. Completar descripción y checklist.

## Checklist de PR
- [ ] Código compila / script CLI funciona
- [ ] \pytest\ en verde
- [ ] README/Docs actualizados si aplica
- [ ] No se sube \.venv/\ ni \*.egg-info/\

## Estilo
- Python 3.10+, tipado cuando sea posible.
- Evitá dependencias pesadas si no son necesarias.
