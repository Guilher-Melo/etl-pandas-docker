FROM python:3.10-slim

WORKDIR /app

COPY pyproject.toml poetry.lock* ./

RUN pip install poetry

# Instale as dependências do projeto
RUN poetry install

# Copie o restante do código do projeto para o diretório de trabalho
COPY . .

# Comando para executar o script principal
CMD ["poetry", "run", "python", "main.py"]
