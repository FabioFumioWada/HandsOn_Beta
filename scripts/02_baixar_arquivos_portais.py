#!/usr/bin/env python3
"""Baixa arquivos públicos de ANEEL, ONS, IBGE e CCEE para a landing zone.

O script foi desenhado para execução local, em um job do Databricks ou em um
Databricks Repo. Ele não fixa anos, nomes ou quantidade de arquivos: consulta
os catálogos em cada execução e grava um manifesto para permitir reexecução
idempotente.

Exemplo no Databricks (cluster com Unity Catalog e acesso à Volume):
    %sh
    python /Workspace/Repos/<usuario>/HandsOn_Beta/scripts/baixar_arquivos_portais.py

As bibliotecas utilizadas são da biblioteca padrão, exceto ``requests``, que
normalmente já está disponível no runtime Databricks.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from email.message import Message
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence
from urllib.parse import unquote, urldefrag, urljoin, urlsplit

try:
    import requests
    from requests import Response, Session
    from requests.exceptions import RequestException
except ImportError as exc:  # pragma: no cover - mensagem operacional
    raise SystemExit(
        "A biblioteca requests não está disponível. No Databricks, execute "
        "%pip install requests e reinicie o Python antes de executar o script."
    ) from exc


SCRIPT_VERSION = "1.0.2"
DEFAULT_OUTPUT_DIR = "/Volumes/handson_beta/landing_zone/arquivos/"
DEFAULT_IBGE_PAGE_URL = (
    "https://www.ibge.gov.br/estatisticas/sociais/populacao/"
    "9103-estimativas-de-populacao.html"
)
DEFAULT_IBGE_ROOT_URL = "https://ftp.ibge.gov.br/Estimativas_de_Populacao/"
USER_AGENT = (
    "HandsOn-Beta-Dados/1.0 "
    "(download automatizado de dados abertos; contato via projeto HandsOn)"
)
RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
FILENAME_FALLBACKS = {"", "content", "download", "downloadfile", "index.html"}


@dataclass(frozen=True)
class Resource:
    """Representa um arquivo encontrado em um catálogo público."""

    platform: str
    url: str
    original_name: str
    dataset: str
    format: str = ""
    resource_id: str = ""
    remote_signature: str = ""
    description: str = ""

    @property
    def key(self) -> str:
        return f"{self.platform}|{self.url}"


class LinkParser(HTMLParser):
    """Extrai links de uma página HTML sem depender de BeautifulSoup."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for key, value in attrs:
            if key.lower() == "href" and value:
                self.links.append(value.strip())
                break


class Throttle:
    """Aplica espera mínima e jitter antes de cada requisição HTTP."""

    def __init__(self, minimum_delay: float, jitter: float) -> None:
        self.minimum_delay = max(0.0, minimum_delay)
        self.jitter = max(0.0, jitter)
        self._last_request = 0.0

    def wait(self) -> None:
        elapsed = time.monotonic() - self._last_request
        remaining = self.minimum_delay - elapsed
        if remaining > 0:
            time.sleep(remaining)
        if self.jitter > 0:
            time.sleep(random.uniform(0, self.jitter))
        self._last_request = time.monotonic()


class JsonlLogger:
    """Mantém uma trilha simples e legível de cada evento da ingestão."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, event: str, **data: Any) -> None:
        record = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "event": event,
            **data,
        }
        with self.path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


class CatalogError(RuntimeError):
    """Erro ao consultar ou interpretar um catálogo."""


class Downloader:
    """Coordena descoberta, download, controle de ritmo e manifesto."""

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.output_dir = Path(args.output_dir).expanduser().resolve()
        self.control_dir = self.output_dir / "_controle"
        self.temp_dir = self.control_dir / "_temporarios"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.control_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        self.session: Session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "application/json, text/html, */*",
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
                "Connection": "keep-alive",
            }
        )
        self.throttle = Throttle(args.intervalo_segundos, args.jitter_segundos)
        self.logger = JsonlLogger(
            self.control_dir
            / f"download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        )
        self.manifest_path = self.control_dir / "manifest.json"
        self.manifest: dict[str, Any] = self._load_manifest()
        self.discovery_errors: list[dict[str, str]] = []
        self.downloaded = 0
        self.skipped = 0
        self.failed = 0

    def _load_manifest(self) -> dict[str, Any]:
        if not self.manifest_path.exists():
            return {"script_version": SCRIPT_VERSION, "entries": {}}
        try:
            content = json.loads(self.manifest_path.read_text(encoding="utf-8"))
            if not isinstance(content, dict) or not isinstance(content.get("entries"), dict):
                raise ValueError("estrutura do manifesto inválida")
            return content
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            logging.warning("Manifesto inválido; será recriado: %s", exc)
            return {"script_version": SCRIPT_VERSION, "entries": {}}

    def _save_manifest(self) -> None:
        temporary = self.manifest_path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(self.manifest, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        os.replace(temporary, self.manifest_path)

    def _request(self, method: str, url: str, **kwargs: Any) -> Response:
        """Faz uma requisição com retry exponencial e respeito a Retry-After."""
        timeout = kwargs.pop("timeout", (self.args.timeout, self.args.timeout))
        last_error: Exception | None = None

        for attempt in range(1, self.args.tentativas + 1):
            self.throttle.wait()
            response: Response | None = None
            try:
                response = self.session.request(
                    method,
                    url,
                    timeout=timeout,
                    allow_redirects=True,
                    **kwargs,
                )
                if response.status_code in RETRYABLE_STATUS_CODES:
                    retry_after = response.headers.get("Retry-After")
                    response.close()
                    delay = self._retry_delay(attempt, retry_after)
                    logging.warning(
                        "HTTP %s em %s (tentativa %s/%s); nova tentativa em %.1fs",
                        response.status_code,
                        url,
                        attempt,
                        self.args.tentativas,
                        delay,
                    )
                    if attempt < self.args.tentativas:
                        time.sleep(delay)
                        continue
                    raise CatalogError(
                        f"HTTP {response.status_code} após {self.args.tentativas} tentativas: {url}"
                    )
                response.raise_for_status()
                return response
            except (RequestException, CatalogError) as exc:
                last_error = exc
                if response is not None:
                    status_code = response.status_code
                    response.close()
                    if status_code >= 400 and status_code not in RETRYABLE_STATUS_CODES:
                        break
                if attempt >= self.args.tentativas:
                    break
                delay = self._retry_delay(attempt, None)
                logging.warning(
                    "Falha em %s %s (tentativa %s/%s): %s; nova tentativa em %.1fs",
                    method,
                    url,
                    attempt,
                    self.args.tentativas,
                    exc,
                    delay,
                )
                time.sleep(delay)

        raise CatalogError(f"Não foi possível acessar {url}: {last_error}") from last_error

    @staticmethod
    def _retry_delay(attempt: int, retry_after: str | None) -> float:
        if retry_after:
            try:
                return min(300.0, max(1.0, float(retry_after)))
            except ValueError:
                pass
        return min(300.0, (2 ** (attempt - 1)) + random.uniform(0, 1))

    def _get_json(self, url: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        response = self._request("GET", url, params=params)
        try:
            payload = response.json()
        except ValueError as exc:
            raise CatalogError(f"Resposta não-JSON em {response.url}") from exc
        finally:
            response.close()
        if not isinstance(payload, dict):
            raise CatalogError(f"JSON inesperado em {url}")
        return payload

    def discover(self) -> list[Resource]:
        resources: list[Resource] = []
        selected = self.args.fontes or ["ANEEL", "ONS", "IBGE", "CCEE"]
        for platform in selected:
            try:
                if platform == "ANEEL":
                    resources.extend(
                        self.discover_ckan_dataset(
                            platform="ANEEL",
                            base_url="https://dadosabertos.aneel.gov.br",
                            dataset_id="bandeiras-tarifarias",
                        )
                    )
                elif platform == "ONS":
                    resources.extend(
                        self.discover_ckan_dataset(
                            platform="ONS",
                            base_url="https://dados.ons.org.br",
                            dataset_id="ena-diario-por-reservatorio",
                        )
                    )
                elif platform == "CCEE":
                    resources.extend(self.discover_ccee())
                elif platform == "IBGE":
                    resources.extend(self.discover_ibge())
            except Exception as exc:  # cada fonte falha de forma independente
                message = str(exc)
                logging.exception("Falha na descoberta da fonte %s", platform)
                self.discovery_errors.append({"platform": platform, "error": message})
                self.logger.write("discovery_error", platform=platform, error=message)

        unique: dict[str, Resource] = {}
        for resource in resources:
            unique.setdefault(resource.key, resource)
        result = list(unique.values())
        self.logger.write("discovery_finished", resource_count=len(result))
        return result

    def discover_ckan_dataset(
        self, platform: str, base_url: str, dataset_id: str
    ) -> list[Resource]:
        endpoint = f"{base_url.rstrip('/')}/api/3/action/package_show"
        payload = self._get_json(endpoint, params={"id": dataset_id})
        if payload.get("success") is not True:
            raise CatalogError(f"CKAN não retornou success=true para {dataset_id}")
        result = payload.get("result") or {}
        resources: list[Resource] = []
        for item in result.get("resources", []):
            resource = self._resource_from_ckan(platform, dataset_id, item)
            if resource:
                resources.append(resource)
        logging.info("%s/%s: %s recursos descobertos", platform, dataset_id, len(resources))
        return resources

    def discover_ccee(self) -> list[Resource]:
        endpoint = "https://dadosabertos.ccee.org.br/api/3/action/package_search"
        resources: list[Resource] = []
        start = 0
        page_size = max(1, min(100, self.args.pagina_ccee))
        max_datasets = self.args.max_datasets_ccee

        while True:
            payload = self._get_json(
                endpoint,
                params={"rows": page_size, "start": start},
            )
            if payload.get("success") is not True:
                raise CatalogError("CKAN da CCEE não retornou success=true")
            result = payload.get("result") or {}
            datasets = result.get("results") or []
            total = int(result.get("count") or 0)
            if not datasets:
                break

            for dataset in datasets:
                if max_datasets and start + datasets.index(dataset) >= max_datasets:
                    break
                dataset_id = str(dataset.get("name") or dataset.get("id") or "dataset")
                for item in dataset.get("resources", []):
                    resource = self._resource_from_ckan("CCEE", dataset_id, item)
                    if resource:
                        resources.append(resource)

            processed = start + len(datasets)
            logging.info(
                "CCEE: página processada (%s/%s conjuntos; %s recursos acumulados)",
                min(processed, total),
                total,
                len(resources),
            )
            if (max_datasets and processed >= max_datasets) or processed >= total:
                break
            start += page_size

        logging.info("CCEE: %s recursos descobertos", len(resources))
        return resources

    def _resource_from_ckan(
        self, platform: str, dataset_id: str, item: Mapping[str, Any]
    ) -> Resource | None:
        url = str(item.get("url") or "").strip()
        if not url.startswith(("http://", "https://")):
            return None
        resource_id = str(item.get("id") or "")
        resource_format = str(item.get("format") or "").strip()
        url_name = self._filename_from_url(url)
        display_name = str(item.get("name") or "").strip()
        original_name = url_name if self._is_meaningful_filename(url_name) else display_name
        if not original_name:
            original_name = f"recurso-{resource_id or hashlib.sha1(url.encode()).hexdigest()[:12]}"
        original_name = self._ensure_extension(original_name, resource_format)
        signature = self._remote_signature(item)
        return Resource(
            platform=platform,
            url=url,
            original_name=original_name,
            dataset=dataset_id,
            format=resource_format,
            resource_id=resource_id,
            remote_signature=signature,
            description=str(item.get("description") or ""),
        )

    def discover_ibge(self) -> list[Resource]:
        """Descobre todos os arquivos do diretório FTP oficial do IBGE.

        A página institucional é consultada apenas para aproveitar links que
        estejam disponíveis. O diretório FTP é a fonte de fallback oficial e
        é percorrido recursivamente, evitando dependência de uma página que,
        em alguns ambientes, responde com bloqueio ao cliente automatizado.
        """
        resources: list[Resource] = []
        page_url = self.args.ibge_page_url
        if page_url:
            try:
                response = self._request("GET", page_url)
                html = response.text
                response.close()
                for url in self._links_from_html(html, page_url):
                    if self._looks_like_download(url):
                        resources.append(
                            Resource(
                                platform="IBGE",
                                url=url,
                                original_name=self._filename_from_url(url),
                                dataset="estimativas-de-populacao",
                                format=Path(urlsplit(url).path).suffix.lstrip(".").upper(),
                            )
                        )
            except Exception as exc:
                logging.warning(
                    "Página institucional do IBGE indisponível; usando FTP oficial: %s", exc
                )
                self.logger.write("ibge_page_warning", url=page_url, error=str(exc))

        visited: set[str] = set()
        ftp_resources = list(
            self._crawl_ibge_directory(
                self.args.ibge_root_url.rstrip("/") + "/", visited=visited, depth=0
            )
        )
        resources.extend(ftp_resources)
        logging.info("IBGE: %s arquivos descobertos no FTP oficial", len(ftp_resources))
        return resources

    def _crawl_ibge_directory(
        self, url: str, visited: set[str], depth: int
    ) -> Iterator[Resource]:
        if url in visited or depth > self.args.ibge_max_depth:
            return
        visited.add(url)
        response = self._request("GET", url)
        try:
            links = self._links_from_html(response.text, url)
        finally:
            response.close()

        for link in links:
            child, _fragment = urldefrag(link)
            if not child.startswith(self.args.ibge_root_url):
                continue
            path = urlsplit(child).path
            if path.rstrip("/") == urlsplit(url).path.rstrip("/"):
                continue
            if child.endswith("/"):
                yield from self._crawl_ibge_directory(child, visited, depth + 1)
            elif self._looks_like_download(child):
                name = self._filename_from_url(child)
                if self._is_meaningful_filename(name):
                    yield Resource(
                        platform="IBGE",
                        url=child,
                        original_name=name,
                        dataset="estimativas-de-populacao",
                        format=Path(name).suffix.lstrip(".").upper(),
                    )

    @staticmethod
    def _links_from_html(html: str, base_url: str) -> list[str]:
        parser = LinkParser()
        parser.feed(html)
        result: list[str] = []
        for href in parser.links:
            if href.startswith(("#", "mailto:", "javascript:")):
                continue
            result.append(urldefrag(urljoin(base_url, href))[0])
        return result

    @staticmethod
    def _looks_like_download(url: str) -> bool:
        name = Path(unquote(urlsplit(url).path)).name.lower()
        return bool(name and name not in FILENAME_FALLBACKS and "." in name)

    @staticmethod
    def _filename_from_url(url: str) -> str:
        name = Path(unquote(urlsplit(url).path)).name
        return name.strip()

    @staticmethod
    def _is_meaningful_filename(name: str) -> bool:
        return bool(name and name.lower() not in FILENAME_FALLBACKS and "." in name)

    @staticmethod
    def _ensure_extension(name: str, resource_format: str) -> str:
        if Path(name).suffix:
            return name
        extension = re.sub(r"[^A-Za-z0-9]", "", resource_format).lower()
        return f"{name}.{extension}" if extension else name

    @staticmethod
    def _remote_signature(item: Mapping[str, Any]) -> str:
        values = [item.get("hash"), item.get("last_modified"), item.get("size")]
        return "|".join(str(value) for value in values if value not in (None, ""))

    def download_all(self, resources: Sequence[Resource]) -> None:
        self.logger.write("download_started", resource_count=len(resources))
        for position, resource in enumerate(resources, start=1):
            try:
                result = self.download_one(resource, position, len(resources))
                if result == "downloaded":
                    self.downloaded += 1
                else:
                    self.skipped += 1
            except Exception as exc:
                self.failed += 1
                logging.exception("Falha no download de %s", resource.url)
                self.logger.write(
                    "download_error",
                    platform=resource.platform,
                    dataset=resource.dataset,
                    url=resource.url,
                    error=str(exc),
                )
                self._set_manifest_entry(
                    resource,
                    {
                        "status": "error",
                        "error": str(exc),
                        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                    },
                )
                if self.args.parar_no_erro:
                    raise

    def download_one(self, resource: Resource, position: int, total: int) -> str:
        entry = self.manifest["entries"].get(resource.key, {})
        if self._can_skip(resource, entry):
            logging.info("[%s/%s] já existe: %s", position, total, entry["local_path"])
            self.logger.write("download_skipped", url=resource.url, local_path=entry["local_path"])
            return "skipped"

        logging.info("[%s/%s] baixando %s", position, total, resource.url)
        response = self._request("GET", resource.url, stream=True)
        temp_path = self.temp_dir / f"{hashlib.sha1(resource.url.encode()).hexdigest()}.part"
        digest = hashlib.sha256()
        byte_count = 0
        try:
            with temp_path.open("wb") as file:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    file.write(chunk)
                    digest.update(chunk)
                    byte_count += len(chunk)
        finally:
            response.close()

        if byte_count == 0:
            temp_path.unlink(missing_ok=True)
            raise IOError(f"resposta vazia para {resource.url}")

        remote_size = self._as_positive_int(resource.remote_signature)
        if remote_size is not None and remote_size != byte_count:
            temp_path.unlink(missing_ok=True)
            raise IOError(
                f"tamanho recebido ({byte_count}) diferente do catálogo ({remote_size})"
            )

        sha256 = digest.hexdigest()
        # O nome do catálogo/URL é usado para reexecuções estáveis e para
        # preservar o nome original informado pela fonte.
        local_path = self._target_path(resource, entry, sha256)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        os.replace(temp_path, local_path)

        manifest_entry = {
            "status": "success",
            "platform": resource.platform,
            "dataset": resource.dataset,
            "resource_id": resource.resource_id,
            "url": resource.url,
            "original_name": resource.original_name,
            "local_path": str(local_path.relative_to(self.output_dir)),
            "bytes": byte_count,
            "sha256": sha256,
            "remote_signature": resource.remote_signature,
            "format": resource.format,
            "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        self._set_manifest_entry(resource, manifest_entry)
        self.logger.write(
            "download_success",
            platform=resource.platform,
            dataset=resource.dataset,
            url=resource.url,
            local_path=manifest_entry["local_path"],
            bytes=byte_count,
            sha256=sha256,
        )
        return "downloaded"

    def _can_skip(self, resource: Resource, entry: Mapping[str, Any]) -> bool:
        if self.args.forcar or entry.get("status") != "success":
            return False
        relative = entry.get("local_path")
        if not relative:
            return False
        path = self.output_dir / str(relative)
        if not path.is_file():
            return False
        stored_signature = str(entry.get("remote_signature") or "")
        if resource.remote_signature and stored_signature != resource.remote_signature:
            return False
        return True

    def _target_path(
        self, resource: Resource, entry: Mapping[str, Any], sha256: str
    ) -> Path:
        previous = entry.get("local_path")
        if previous and not self.args.forcar:
            candidate = self.output_dir / str(previous)
            if candidate.parent == self.output_dir and candidate.name.startswith(resource.platform + "-"):
                return candidate
        filename = f"{resource.platform}-{self._sanitize_filename(resource.original_name)}"
        candidate = self.output_dir / filename
        if candidate.exists() and not self._same_manifest_path(candidate, resource.key):
            candidate = candidate.with_name(
                f"{candidate.stem}__{sha256[:8]}{candidate.suffix}"
            )
        counter = 2
        while candidate.exists() and not self._same_manifest_path(candidate, resource.key):
            candidate = self.output_dir / (
                f"{resource.platform}-{self._sanitize_filename(resource.original_name)}"
                f"__{sha256[:8]}_{counter}{Path(filename).suffix}"
            )
            counter += 1
        return candidate

    def _same_manifest_path(self, path: Path, resource_key: str) -> bool:
        relative = str(path.relative_to(self.output_dir))
        entry = self.manifest["entries"].get(resource_key, {})
        return entry.get("local_path") == relative

    def _set_manifest_entry(self, resource: Resource, values: Mapping[str, Any]) -> None:
        current = dict(self.manifest["entries"].get(resource.key, {}))
        current.update(values)
        self.manifest["entries"][resource.key] = current
        self.manifest["script_version"] = SCRIPT_VERSION
        self._save_manifest()

    @staticmethod
    def _sanitize_filename(value: str) -> str:
        value = unquote(str(value)).strip()
        value = re.sub(r'[\x00-\x1f<>:"/\\|?*]+', "_", value)
        value = re.sub(r"\s+", " ", value)
        value = value.strip(" .")
        return value or "arquivo_sem_nome"

    @staticmethod
    def _filename_from_response(
        response_headers: Mapping[str, str], fallback: str
    ) -> str:
        content_disposition = response_headers.get("Content-Disposition", "")
        if content_disposition:
            message = Message()
            message["content-disposition"] = content_disposition
            filename = message.get_param("filename", header="content-disposition")
            if filename:
                return unquote(str(filename).strip('"'))
            match = re.search(r"filename\\*?=(?:UTF-8'')?([^;]+)", content_disposition, re.I)
            if match:
                return unquote(match.group(1).strip().strip('"'))
        return fallback

    @staticmethod
    def _as_positive_int(signature: str) -> int | None:
        # A assinatura pode conter hash|last_modified|size; somente converte
        # quando o último segmento é exclusivamente numérico.
        if not signature:
            return None
        last = signature.split("|")[-1]
        return int(last) if last.isdigit() else None

    def save_catalog_snapshot(self, resources: Sequence[Resource]) -> Path:
        path = self.control_dir / "catalogo_descoberto.json"
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "script_version": SCRIPT_VERSION,
            "resources": [asdict(item) | {"key": item.key} for item in resources],
            "discovery_errors": self.discovery_errors,
        }
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return path

    def save_summary(self, resources: Sequence[Resource]) -> Path:
        path = self.control_dir / "ultimo_resumo.json"
        payload = {
            "finished_at_utc": datetime.now(timezone.utc).isoformat(),
            "script_version": SCRIPT_VERSION,
            "output_dir": str(self.output_dir),
            "resources_discovered": len(resources),
            "downloaded": self.downloaded,
            "skipped": self.skipped,
            "failed": self.failed,
            "discovery_errors": self.discovery_errors,
            "manifest_path": str(self.manifest_path),
        }
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return path


def is_interactive_kernel() -> bool:
    """Detecta o launcher interativo que injeta argumentos técnicos.

    O Databricks pode iniciar o processo por ``db_ipykernel_launcher.py`` e
    acrescentar ``-f <connection.json>`` ao ``sys.argv``. Essa detecção é usada
    somente para remover esse argumento técnico; o parser continua estrito em
    execução normal por terminal, Job ou CI/CD.
    """

    launcher = Path(sys.argv[0]).name.lower() if sys.argv else ""
    return (
        "ipykernel" in launcher
        or "ipykernel" in sys.modules
        or bool(os.getenv("DATABRICKS_NOTEBOOK_ID"))
    )


def is_databricks_connection_file(value: str) -> bool:
    """Identifica o caminho técnico de conexão usado pelo Databricks.

    O launcher observado em clusters Databricks usa caminhos como
    ``/local_disk0/sandboxapi/<id>/connection.json``. A verificação é
    intencionalmente específica para não transformar qualquer ``-f`` em uma
    opção silenciosamente aceita pela CLI.
    """

    normalized = str(value).replace("\\\\", "/").lower()
    return normalized.endswith("/connection.json") and (
        normalized.startswith("/local_disk0/") or "/sandboxapi/" in normalized
    )


def sanitize_interactive_args(
    argv: Sequence[str], *, interactive: bool | None = None
) -> list[str]:
    """Remove argumentos técnicos do kernel sem ignorar erros reais.

    Não usa ``parse_known_args`` de propósito: argumentos desconhecidos devem
    continuar gerando erro. O par ``-f <valor>`` é removido quando o processo
    é interativo ou quando o valor tem o padrão específico de conexão do
    Databricks. Em execução normal, ``-f`` com qualquer outro valor continua
    sendo rejeitado pelo ``argparse``.
    """

    if interactive is None:
        interactive = is_interactive_kernel()

    sanitized: list[str] = []
    index = 0
    while index < len(argv):
        argument = argv[index]
        if argument in {"-f", "--connection-file"}:
            next_value = argv[index + 1] if index + 1 < len(argv) else ""
            is_kernel_argument = bool(interactive) or is_databricks_connection_file(
                next_value
            )
            if is_kernel_argument:
                # O valor seguinte é o caminho do arquivo de conexão do kernel.
                index += 2 if index + 1 < len(argv) else 1
                continue
        if argument.startswith("--connection-file="):
            connection_value = argument.split("=", 1)[1]
            if bool(interactive) or is_databricks_connection_file(connection_value):
                index += 1
                continue
        sanitized.append(argument)
        index += 1
    return sanitized


def parse_args(
    argv: Sequence[str] | None = None, *, interactive: bool | None = None
) -> argparse.Namespace:
    """Lê argumentos de terminal ou do launcher interativo do Databricks."""

    raw_args = sys.argv[1:] if argv is None else argv
    cleaned_args = sanitize_interactive_args(raw_args, interactive=interactive)
    return build_parser().parse_args(cleaned_args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Descobre e baixa arquivos públicos de ANEEL, ONS, IBGE e CCEE."
    )
    parser.add_argument(
        "--fontes",
        nargs="+",
        choices=["ANEEL", "ONS", "IBGE", "CCEE"],
        help="Fontes a processar. O padrão é processar as quatro fontes.",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Diretório de destino (padrão: {DEFAULT_OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--intervalo-segundos",
        type=float,
        default=5.0,
        help="Espera mínima entre requisições (padrão: 5 segundos).",
    )
    parser.add_argument(
        "--jitter-segundos",
        type=float,
        default=2.0,
        help="Espera aleatória adicional entre requisições (padrão: 2 segundos).",
    )
    parser.add_argument(
        "--tentativas",
        type=int,
        default=5,
        help="Número de tentativas para falhas transitórias (padrão: 5).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="Timeout de conexão e leitura em segundos (padrão: 120).",
    )
    parser.add_argument(
        "--pagina-ccee",
        type=int,
        default=50,
        help="Quantidade de conjuntos por página da API CCEE (máximo 100).",
    )
    parser.add_argument(
        "--max-datasets-ccee",
        type=int,
        default=0,
        help="Limite de conjuntos CCEE; 0 significa todos (padrão: 0).",
    )
    parser.add_argument(
        "--ibge-page-url",
        default=DEFAULT_IBGE_PAGE_URL,
        help="Página institucional do IBGE usada como tentativa complementar.",
    )
    parser.add_argument(
        "--ibge-root-url",
        default=DEFAULT_IBGE_ROOT_URL,
        help="Diretório FTP oficial do IBGE usado como fonte de arquivos.",
    )
    parser.add_argument(
        "--ibge-max-depth",
        type=int,
        default=3,
        help="Profundidade máxima de diretórios no FTP do IBGE (padrão: 3).",
    )
    parser.add_argument(
        "--forcar",
        action="store_true",
        help="Baixa novamente mesmo quando o manifesto indica que o arquivo está atualizado.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Somente descobre e grava o catálogo; não baixa arquivos.",
    )
    parser.add_argument(
        "--parar-no-erro",
        action="store_true",
        help="Interrompe ao primeiro erro de download; por padrão continua com os demais.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.tentativas < 1:
        raise SystemExit("--tentativas deve ser maior ou igual a 1")
    if args.intervalo_segundos < 0 or args.jitter_segundos < 0:
        raise SystemExit("intervalos não podem ser negativos")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.info("Início da ingestão; versão %s", SCRIPT_VERSION)
    downloader = Downloader(args)
    resources = downloader.discover()
    catalog_path = downloader.save_catalog_snapshot(resources)
    logging.info("Catálogo salvo em %s", catalog_path)

    if not args.dry_run:
        downloader.download_all(resources)
    summary_path = downloader.save_summary(resources)
    logging.info(
        "Fim: descobertos=%s, baixados=%s, ignorados=%s, falhas=%s",
        len(resources),
        downloader.downloaded,
        downloader.skipped,
        downloader.failed,
    )
    logging.info("Resumo salvo em %s", summary_path)

    if downloader.discovery_errors or downloader.failed:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
