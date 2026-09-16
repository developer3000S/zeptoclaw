# Ansible: массовое развёртывание ZeptoClaw Agent Mesh (ТЗ 15.4, ТЗ 22 п.7)

Здесь — средства развернуть mesh-узлы **на множестве серверов** (100+), а не на
одной машине. Закрывает все шесть пунктов ТЗ 15.4:

| # | Пункт ТЗ 15.4 | Чем закрыт |
|---|---------------|-----------|
| 1 | Установить Docker | `roles/zeptomesh/tasks/docker.yml` (тег `docker`) |
| 2 | Создать каталоги | `…/tasks/dirs.yml` (тег `dirs`) |
| 3 | Сгенерировать/доставить конфигурацию | `…/tasks/config.yml` + `templates/instance.env.j2` (тег `config`) |
| 4 | Запустить контейнер | `…/tasks/run.yml` + `…/tasks/image.yml` (теги `run`, `image`) |
| 5 | Проверить здоровье узла | `…/tasks/health.yml` (тег `health`) — HTTP-API узла |
| 6 | Собрать начальные Peer ID | `…/tasks/collect.yml` + три шаблона артефактов (тег `peerids`) |

Плюс шаг 0 — `preflight.yml` (тег `preflight`/`always`): проверки, которые
ловят ошибки конфигурации **до** любых изменений на хосте.

---

## Быстрый старт

```bash
cd deploy/ansible

# 1. Инвентарь: скопировать пример и вписать свои хосты/адреса.
cp inventory.example.yml inventory.yml
$EDITOR inventory.yml

# 2. Секреты (PSK, токены) — в ansible-vault, не в инвентарь.
mkdir -p group_vars/mesh_nodes
cp .vault.example.yml group_vars/mesh_nodes/vault.yml
ansible-vault encrypt group_vars/mesh_nodes/vault.yml
ansible-vault edit   group_vars/mesh_nodes/vault.yml

# 3. Обязательно посмотреть, что будет, не меняя ничего.
ansible-playbook -i inventory.yml --check --diff deploy.yml \
    --ask-vault-pass -e zeptomesh_image_source=none

# 4. Первая партия — якоря. Соседей у них по определению нет, поэтому
#    требование соседей отключаем, иначе шаг 5 прервёт прогон.
ansible-playbook -i inventory.yml deploy.yml --ask-vault-pass \
    --limit anchors -e zeptomesh_require_peers=false

# 5. Собрать их Peer ID -> артефакт на контроллере.
ansible-playbook -i inventory.yml collect_peerids.yml --ask-vault-pass \
    --limit anchors
cat collected/mesh_nodes.bootstrap.txt      # готовое значение ZETOMESH_BOOTSTRAP

# 6. Остальные узлы: роль подставит точки входа сама (zeptomesh_auto_bootstrap).
ansible-playbook -i inventory.yml deploy.yml --ask-vault-pass \
    --limit workers
```

Из каталога `deploy/ansible/` подхватываются `ansible.cfg` (инвентарь по
умолчанию, `roles_path = roles`) и `roles/`. Если запускаете из другого
каталога — укажите `-i` и путь к плейбуку явно; `ANSIBLE_CONFIG` при этом не
обязателен, все пути внутри роли разрешаются через `playbook_dir`.

Проверка синтаксиса и линтер:

```bash
ansible-playbook -i inventory.example.yml deploy.yml --syntax-check
ansible-playbook -i inventory.example.yml collect_peerids.yml --syntax-check
ansible-lint deploy.yml collect_peerids.yml
```

---

## Что делает каждый плейбук

### `deploy.yml` — полный цикл (шаги 1–6)

Один плей, `hosts: mesh_nodes`, `become: true`, `serial: {{ zeptomesh_serial | default(25) }}`
— хосты идут партиями по 25, чтобы падение одного узла не останавливало прогон
и не перекрывало сеть целиком. Порядок задач:

1. **preflight** — количество экземпляров (1–64), непересекающиеся диапазоны
   портов, допустимость политик (`trust_mode`, `pico_mode`, `dht_mode`,
   `image_source`), пригодность префиксов имён для контейнера/hostname/тома,
   наличие источника конфигурации на контроллере, формат bootstrap-адресов,
   формат PSK, формат peer id в allow/block-списках, `zeptomesh_pub_ip` для
   профилей с announce, наличие Dockerfile на хосте при `image_source=build`.
2. **plan** — раскладка экземпляров хоста (см. ниже).
3. **docker** → **dirs** → **config** → **image** → **run** → **health** →
   **peerids** (шаг 6 пишет артефакт, если `zeptomesh_write_artifact: true`).

Любой шаг можно вызвать отдельно тегами:

```bash
ansible-playbook -i inventory.yml deploy.yml --tags config --ask-vault-pass
ansible-playbook -i inventory.yml deploy.yml --tags health,peerids
ansible-playbook -i inventory.yml deploy.yml --skip-tags docker   # демон уже есть
```

### `collect_peerids.yml` — только шаги 5–6

Тот же плей, но с `zeptomesh_stage: collect`: на хосты не пишет ничего, только
опрашивает API узлов и складывает артефакты на контроллер. Нужен, когда узлы
уже развернуты (или развернуты вручную/через compose) и надо получить точки
входа для следующей партии. `zeptomesh_no_log: false` — собранные адреса не
секреты, а сводку здоровью видно оператору.

---

## Почему роль, а не плоский плейбук

Все шесть шагов нужны **двум** плейбукам: `deploy.yml` (полный цикл) и
`collect_peerids.yml` (только здоровье + Peer ID). Дублировать 500 строк
задач в двух файлах — значит однажды развести их так, что collect проверяет
здоровье иначе, чем deploy. Поэтому логика живёт в одной роли
`roles/zeptomesh`, а плейбуки — тонкие обёртки, отличающиеся одной
переменной `zeptomesh_stage` (`deploy` | `collect`) и набором тегов.

Структура роли:

```
roles/zeptomesh/
├── defaults/main.yml   # все настройки оператора (комментированы) — перекрываются
│                       # group_vars/host_vars/-e
├── meta/main.yml       # min_ansible_version 2.14, платформы Debian/Ubuntu/EL
├── handlers/main.yml   # один: «Перезапустить docker»
├── tasks/              # main.yml — диспетчер по zeptomesh_stage + теги шагов
│   ├── preflight.yml   ├── plan.yml      ├── docker.yml
│   ├── dirs.yml        ├── config.yml    ├── image.yml
│   ├── run.yml         ├── health.yml    └── collect.yml
└── templates/
    ├── instance.env.j2       # окружение экземпляра (PSK/токены, 0600)
    ├── peerids.yml.j2        # артефакт: {хост: {экземпляр: [multiaddr]}}
    ├── bootstrap.txt.j2      # артефакт: одна строка — ZETOMESH_BOOTSTRAP
    └── peerids.csv.j2        # артефакт: человекочитаемые детали
```

Замечание по `defaults`: выводные величины (пути в контейнере, `zeptomesh_image`,
`zeptomesh_volume_prefix`, apt/yum-URL репозитория Docker) тоже живут в
`defaults/main.yml`, а не в `vars/main.yml`: переменные роли из `vars/`
имеют **более высокий** приоритет, чем `group_vars` и `-e`, и оператор не смог
бы их переопределить; кроме того ansible подхватывает из `vars/` только
`main.yml`.

---

## Планировка экземпляров и формат артефакта

Экземпляр `i` на хосте получает ровно те же порты, что `install.sh` и
`deploy/docker/docker-compose.yml` (формула вынесена в `tasks/plan.yml`, и все
шаги — конфиг, запуск, здоровье, сбор — идут по одному и тому же списку,
поэтому порты/тома/имена не могут разойтись между шагами):

| что | порт | публикация |
|-----|------|-----------|
| mesh (libp2p) | `zeptomesh_base_mesh_port + i` (по умолчанию 4001+i) | `0.0.0.0`, **tcp и udp** (QUIC + резервный TCP) |
| админ-API | 8081+i | только `127.0.0.1` |
| метрики | 9464+i | только `127.0.0.1`, при `zeptomesh_prometheus_enabled` |

Один параметризованный `node.yaml` обслуживает все N узлов хоста: узел сам
раскрывает `${VAR:-default}` (`internal/config.expandEnv`) и дописывает
`ZETOMESH_BOOTSTRAP` в `discovery.bootstrap` (`config.applyEnvOverrides`), так
что отличия экземпляров живут в env-файлах, а не в копиях конфигурации.

Артефакты в `collected/` (по одному на группу, перезаписываются полным
составом группы):

```
collected/mesh_nodes.peerids.yml     # роль читает его обратно (автоbootstrap)
collected/mesh_nodes.bootstrap.txt   # одна строка = значение ZETOMESH_BOOTSTRAP
collected/mesh_nodes.peerids.csv     # хост, экземпляр, peer id, источник, адреса
```

Формат адреса — `/ip4/<addr>/tcp/<port>/p2p/<peer id>`. `bootstrap.txt`
пригоден как есть:

```bash
ansible-playbook -i inventory.yml deploy.yml \
    -e zeptomesh_bootstrap="$(cat collected/mesh_nodes.bootstrap.txt)"
# или тем же значением скормить install.sh:
./install.sh --mode docker --bootstrap "$(cat collected/mesh_nodes.bootstrap.txt)"
```

Приоритет выбора адреса для экземпляра: tcp с публичным IP хоста → любой tcp →
любой анонсированный → синтез из `zeptomesh_pub_ip`. Источник пишется в CSV
колонкой `source` (`announced` / `synthesized`), чтобы оператор видел, где
адрес настоящий, а где достроен.

---

## Секреты

Секретов в репозитории нет. `.vault.example.yml` — шаблон с заглушками.

```bash
mkdir -p group_vars/mesh_nodes
cp .vault.example.yml group_vars/mesh_nodes/vault.yml
ansible-vault encrypt group_vars/mesh_nodes/vault.yml
ansible-vault edit   group_vars/mesh_nodes/vault.yml      # правит в зашифрованном виде
ansible-playbook -i inventory.yml deploy.yml --ask-vault-pass
# либо без интерактива:
ansible-playbook -i inventory.yml deploy.yml --vault-password-file ~/.vault-pass
```

Путь секрета: vault → переменная роли → env-файл экземпляра
`/etc/zeptomesh-env/zeptomesh-<i>.env` (права `0600`, каталог `0750`) → модуль
`community.docker.docker_container` читает файл на хосте и передаёт значения
как `Env` контейнера. **env-файлы в контейнер не монтируются** — иначе узел
`zepto-0` видел бы PSK/токены соседей по хосту. Задачи, оперирующие ими, под
`no_log: zeptomesh_no_log` (по умолчанию `true` в `group_vars/all.yml`).

`zeptomesh_psk` роль проверяет по формату (`/1/<base32 от 32 байт>` — его
разбирает `internal/p2p.decodePSK`; ошибочный формат роняет контейнер на
старте). Генерация: `bin/zeptomesh-node psk` или
`docker run --rm --entrypoint zeptomesh-node zeptomesh-node:0.1.0 psk`.
**Ключ обязан быть одинаковым у всех узлов одной сети**: разные PSK — разные
сети, узлы не увидят друг друга никогда (docs/RUNBOOK.md §4.3).

Доступ к реестру образов (`zeptomesh_image_source: pull`) в роль не входит —
выполните отдельным шагом, пример в `.vault.example.yml`.

Альтернатива vault — незашифрованный файл переменных, передаваемый явно
(роль **не** читает переменные окружения контроллера сама; в CI секреты
подставляют либо файлом, либо `-e`):

```bash
ansible-playbook -i inventory.yml deploy.yml -e @secrets.yml   # файл не под git
# или из secrets-менеджера пайплайна:
ansible-playbook -i inventory.yml deploy.yml \
    -e zeptomesh_psk="$CI_MESH_PSK" -e zeptomesh_api_token="$CI_API_TOKEN"
```

Во всех вариантах значение попадает только в env-файл экземпляра (0600) и в
`Env` контейнера.

---

## Идемпотентность и изменение конфигурации

- Шаг 3 по умолчанию **не перезаписывает** уже правленный на хосте
  `node.yaml` (`zeptomesh_config_force: false`); синхронизировать с источником
  репозитория — `-e zeptomesh_config_force=true`.
- Перезапуск узла при смене конфигурации сделан **не обработчиком**, а меткой
  `com.zeptomesh.config-hash` (sha256 от содержимого конфига):
  `docker_container` сравнивает метки как dict, и изменение значения само
  заставляет модуль пересоздать контейнер. Обработчик же дал бы двойной
  bounce на свежей установке (сработал бы до первого запуска и ничего не
  перезапустил). Единственный обработчик в роли — «Перезапустить docker», он
  нужен после установки/обновления пакетов движка.
- Повторный прогон по работающей сети `changed=0` (проверено на каталогах,
  конфигурации и env-файлах).
- Никаких `latest`: тег образа закреплён (`zeptomesh_image_tag: "0.1.0"`,
  совпадает с `VERSION ?= 0.1.0` в Makefile). Меняется версия — меняется тег,
  и mesh обновляется управляемо, с возможностью откатиться.
- `stop_timeout: 30` + `stop_signal: SIGTERM` — grace-период как в compose
  (в `community.docker` 3.x параметр называется `stop_timeout`, а не
  `stop_grace_period`).

---

## Отличие от `install.sh`

`install.sh` и Ansible решают **разные** задачи и не дублируют друг друга.

| | `install.sh` | Ansible (эта директория) |
|---|---|---|
| Масштаб | одна машина, N экземпляров | M хостов × N экземпляров |
| Где выполняется | локально, от root на своей машине | с контроллера по SSH |
| Docker | предполагает установленным; `--mode docker` пишет compose | устанавливает движок из официального репозитория (шаг 1) |
| Конфигурация | генерирует `.env`/юниты/`compose` на месте | доставляет выбранный профиль + env-файлы, владеет ими декларативно |
| Раздача точек входа | поднимает якорь отдельно, берёт его `peer_id` из `/healthz` и вписывает полный `/dns4/zepto-0/tcp/<порт>/p2p/<id>` остальным (оба режима) | собирает реальные анонсы узлов **через HTTP-API всей группы** и пишет артефакт на контроллер |
| Межхостовые секреты | не предусмотрены | ansible-vault |
| Порядок/партии | нет | `serial`, `--limit`, `max_fail_percentage` |
| Итог | машина работает | 100+ машин работают, а следующая партия получает точки входа автоматически |

`install.sh` остаётся штатным путём для разработки и одиночного стенда
(docs/DEPLOYMENT.md §1); Ansible — путь для развёртывания на многие серверы
(ТЗ 15.4).

**Формат bootstrap-адреса.** Узел отвергает адрес без `/p2p/<peer id>`
(проверено на реальном бинарнике; источник — `internal/discovery/bootstrap.go`):

```
zeptomesh-node: node: bootstrap: discovery: bad bootstrap addrs:
"/dns4/zepto-0/tcp/4001" has no /p2p/<peer id> part and cannot be dialed
```

Так правильно: диалить нечего, а молча проигнорированный адрес означал бы mesh,
который «должен был собраться», но не собрался. Отсюда порядок: peer ID узла
известен только после его первого запуска (выводится из ключа на томе), поэтому
он не может быть заранее зашит в статичный файл.

Здесь это закрывает шаг 6 (`collect_peerids.yml`): плейбук опрашивает
`/api/v1/status` работающей группы и пишет `collected/<группа>.bootstrap.txt` —
готовые полные адреса. Тот же путь прошли `install.sh` (режим docker: якорь
поднимается отдельно, его реальный peer ID подставляется остальным) и
статичный `deploy/docker/docker-compose.yml` (адрес берётся из `ZETOMESH_ANCHOR`;
пусто → узлы сходятся через DHT/PEX). Preflight роли отлавливает битые адреса
до развёртывания, поэтому Ansible-путь не даст поднять сеть с нерабочими
точками входа.

---

## Переменные хоста (кратко)

Полный список с комментариями — `roles/zeptomesh/defaults/main.yml`; в
инвентарии обычно нужны только:

| переменная | зачем |
|-----------|-------|
| `zeptomesh_node_count` | сколько контейнеров-узлов поднять на хосте (1–64) |
| `zeptomesh_base_mesh_port` / `_api_port` / `_prom_port` | базы портов; экземпляр i → база+i |
| `zeptomesh_pub_ip` | публичный IPv4 для announce (профиль wan.yaml) |
| `zeptomesh_config_source` | какой профиль доставлять (`configs/node.yaml`, `examples/lan.yaml`, `examples/wan.yaml`) |
| `zeptomesh_image_source` | `build` (исходники на хосте) / `pull` (реестр) / `none` (образ уже есть) |
| `zeptomesh_image_repository` / `_tag` | образ, тег закреплен |
| `zeptomesh_bootstrap` | точки входа через запятую; пусто → из артефакта группы |
| `zeptomesh_data_bind_mount` + `_data_host_root` + `_data_owner_uid`/`_gid` | данные в каталогах хоста вместо docker-тома |
| `zeptomesh_dht_mode`, `zeptomesh_relay_advertise` | якорь (`server`) или обычный узел |
| `zeptomesh_require_peers` | прерывать ли прогон, если узел без соседей |
| `zeptomesh_serial` | размер партии хостов: число или `30%` (значение `all` в ansible-core 2.14 падает с `invalid literal for int()` — см. комментарий в `group_vars/all.yml`) |

### Каталог данных: том против bind-монтирования

По умолчанию данные — именованный docker-том (`zeptomesh-<хост>-<i>`), как в
compose. Если нужны каталоги на хосте (`zeptomesh_data_bind_mount: true`),
обязательно задать `zeptomesh_data_owner_uid`/`_gid`: контейнер работает под
пользователем из образа (`USER zeptomesh`), его числовой uid **не
гарантирован** и зависит от сборки, а bind-монтирование наследует права
хостового каталога. Роль не угадывает uid, а требует его у оператора и
создаёт каталоги с `0700` (+ `container_file_t`, если на хосте включён
SELinux). Вложенные каталоги (`keys/`, `db/`, `run/`, `workspace/`) узел
создаёт сам — это делает `Config.Validate()` + `node.New` до любых сетевых
действий.

---

## Границы проверки (что НЕ проверялось)

Это важно при приёме работы. Проверенное:

- `--syntax-check` — оба плейбука, чистый вывод (ansible-core 2.14.18);
- `ansible-lint` 6.13.1 — `Passed with production profile: 0 failure(s),
  0 warning(s) on 15 files`;
- `--check` (dry-run) на локальном стенде с реальным Docker-демоном —
  `failed=0` для обоих плейбуков; ничего не создано на хосте (проверено:
  ни томов, ни контейнеров, ни `/etc/zeptomesh*`, ни `docker-ce.list`);
- реальные прогоны шагов 2–3 (каталоги + конфигурация + env) в `/tmp` — с
  проверкой идемпотентности (второй прогон `changed=0`) и разбором
  сгенерированных env-файлов;
- рендер всех трёх артефактов на подставных ответах API (2 хоста × 2
  экземпляра) и **сквозная проверка туда-обратно**: `collect_peerids.yml` →
  `collected/*.peerids.yml` → `deploy.yml` читает его обратно и подставляет
  каждому узлу список **без его собственных** адресов;
- формат ответов API против **живого** узла: `GET /healthz` и
  `GET /api/v1/status` (в том числе наличие `/p2p-circuit` и quic-v1 в
  `addrs`, поведение 401 без токена);
- что узел действительно принимает собранные адреса как
  `ZETOMESH_BOOTSTRAP` (включая `/p2p-circuit` и `quic-v1`) и что адрес без
  `/p2p/` отбивает старт — отсюда проверка формата на preflight.

Что осталось непроверенным:

1. **Настоящий кластер из 100+ хостов.** Никакого межхостового стенда не было:
   `serial`/`--limit`, поведение при частичном отказе партии, `max_fail_percentage`
   и скорость на больших инвентарях — аргументированы, но не измерены.
2. **Реальный запуск контейнеров.** По условиям задачи на этой машине
   контейнеры не поднимались (порты заняты, идут другие процессы). Значит
   шаги 4–6 в живом виде не выполнены: не проверены сборка образа
   (`image_source=build`), публикация портов tcp+udp, `restart_policy`,
   healthcheck контейнера, пересоздание контейнера по `config-hash` и
   фактическое присоединение узлов к mesh (`neighbors_connected > 0`).
3. **Ограничение check-mode**: `community.docker.docker_container` читает
   env-файлы с диска хоста, а `--check` ничего не пишет — поэтому шаг запуска
   в dry-run честно **пропускается** с предупреждением. Dry-run валиден для
   каталогов, конфигурации и окружения, но не моделирует запуск узлов и не
   проверяет здоровье.
4. **Docker на сторонних ОС**: задачи установки написаны для Debian/Ubuntu и
   EL (yum_repository, EPEL); Suse/Arch не описаны — роль на них остановит
   прогон с внятным сообщением и предложением `zeptomesh_docker_install=false`.
   Ни одна из этих веток не выполнялась на чистых машинах.
5. **Только один стенд по одному хосту в инвентаре** (`ansible_connection:
   local`) и инвентарь из двух stub-хостов для артефактов. Многохостовый SSH-путь
   (ProxyJump, разные интерпретаторы python, sudo-требования дистрибутивов) не
   тестировался.
6. **Pull из реестра**: `zeptomesh_image_source: pull` и `docker_login` —
   только код, без обращения к реальному реестру.
7. Ansible 2.14 и `community.docker` 3.x — фиксированные версии этой машины;
   на более новых коллекциях отдельные приёмы (например запись
   apt-источника файлом `sources.list.d` вместо `deb822_repository`, которого
   нет в `community.general` 6.6.2) стоит перепроверить.

Перед боевым прогоном на 100+ хостах: `--check --diff`, затем одна малая
партия (`--limit` + `-e zeptomesh_serial=2`), `collect_peerids.yml`, и только
после этого — остальная сеть.
