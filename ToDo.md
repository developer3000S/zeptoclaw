# ToDo List

## Задачи

1. **Добавить RBAC для админ-API** — ✅ готово
   - Реализован роль-based access control: именованные токены `api.tokens`
     (роли viewer/operator/admin), legacy-токен остаётся полным admin
   - Обновлена документация (README.md, configs/node.yaml, docs/STATUS.md)

2. **Добавить аудит-логирование для изменений и отказов** — ✅ готово
   - Аудит-журнал фиксирует `api_mutation`, `api_auth_denied` (401),
     `api_role_denied` (403) с именем токена (`actor`) и ролью
   - Обновлена документация

3. **Добавить метрики HTTP-запросов и инвентарь задач по статусу** — ✅ готово
   - `zeptomesh_http_requests_total{method,path,status}` + `zeptomesh_task_inventory{status}`;
     инвентарь обновляется узлом каждые 15 c
   - Обновлена документация

4. **Обновить дашборд для отображения версии, времени работы и распределения статусов задач** — ✅ готово
   - В шапке — версия/аптайм (`#version-indicator`); в разделе «Задачи» — панель
     «Распределение по статусам (все узлы)»
   - Обновлена документация (docs/UI.md)

5. **Обновить документацию** — ✅ готово
   - README.md, ToDo.md, docs/STATUS.md, docs/UI.md приведены в соответствие

6. **install.sh: сохранять режим picoclaw при переустановке** — ✅ готово
   - Повторный `install.sh` поверх существующего стека больше не сбрасывает
     `--pico-mode` по умолчанию на `stub`: если режим не задан явно
     (`--pico-mode` или env `ZEPTOMESH_PICO_MODE`), берётся прежний режим
     из `docker-compose.yml` (docker) или `instance.env` (local) — например
     `ollama` — и сохраняется
   - Обновлена документация (README.md, docs/DEPLOYMENT.md)
