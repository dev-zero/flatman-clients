# FLATMAN Clients

To install:

```sh
easy_install \
    --user \
    "git+https://github.com/dev-zero/flatman-clients.git"

export PATH="${HOME}/.local/bin:${PATH}"
```

.. or for development:

```sh
git clone https://github.com/dev-zero/flatman-clients.git
cd flatman-clients

virtualenv venv
. venv/bin/activate
pip install --editable .
```

Afterwards, the following applications will be available:

  * fdaemon .. the work horse to fetch tasks, run them and shuffle back the data
  * fclient .. CLI to query, create and alter data on the server
