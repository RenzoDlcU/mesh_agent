"""
Cliente para Bitbucket Server REST API.

Permite leer archivos, crear branches, hacer commits y crear Pull Requests
en el repositorio de mallas Control-M.

Configuración via variables de entorno:
    BITBUCKET_URL: URL base de Bitbucket Server (sin trailing slash)
    BITBUCKET_PROJECT: Clave del proyecto
    BITBUCKET_REPO: Slug del repositorio
    BITBUCKET_TOKEN: Token de acceso personal (HTTP Access Token)
    BITBUCKET_USER_NAME: Nombre del usuario para los commits de git
    BITBUCKET_USER_EMAIL: Email del usuario para los commits de git
"""

import os
import subprocess
import tempfile
import requests
from typing import Optional


class BitbucketServer:
    """Cliente para Bitbucket Server REST API 1.0."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        project_key: Optional[str] = None,
        repo_slug: Optional[str] = None,
        token: Optional[str] = None,
    ):
        self.base_url = (base_url or os.getenv("BITBUCKET_URL", "")).rstrip("/")
        self.project_key = project_key or os.getenv("BITBUCKET_PROJECT", "")
        self.repo_slug = repo_slug or os.getenv("BITBUCKET_REPO", "")
        self.token = token or os.getenv("BITBUCKET_TOKEN", "")

        self._session = requests.Session()
        self._session.headers["Authorization"] = f"Bearer {self.token}"
        # Desactivar verificación SSL si es un servidor interno con cert propio
        self._session.verify = os.getenv("BITBUCKET_SSL_VERIFY", "true").lower() == "true"

    @staticmethod
    def _raise_with_detail(resp: requests.Response, context: str = "") -> None:
        """raise_for_status incluyendo el body de error de Bitbucket."""
        if resp.ok:
            return
        try:
            body = resp.json()
        except Exception:
            body = resp.text[:500]
        prefix = f"[{context}] " if context else ""
        raise requests.HTTPError(
            f"{prefix}HTTP {resp.status_code} – {body}",
            response=resp,
        )

    @property
    def _api_base(self) -> str:
        return (
            f"{self.base_url}/rest/api/1.0"
            f"/projects/{self.project_key}"
            f"/repos/{self.repo_slug}"
        )

    # ── Lectura ──────────────────────────────────────────────

    def list_files(self, path: str = "", ref: Optional[str] = None) -> list[str]:
        """
        Lista archivos y directorios en una ruta del repo.

        Returns:
            Lista de nombres. Los directorios terminan en '/'.
        """
        url = f"{self._api_base}/browse/{path}"
        params = {"limit": 1000}
        if ref:
            params["at"] = ref

        files = []
        start = 0

        while True:
            params["start"] = start
            resp = self._session.get(url, params=params)
            self._raise_with_detail(resp, "list_files")
            data = resp.json()

            for child in data.get("children", {}).get("values", []):
                name = child["path"]["toString"]
                if child["type"] == "DIRECTORY":
                    files.append(f"{name}/")
                else:
                    files.append(name)

            if data.get("children", {}).get("isLastPage", True):
                break
            start = data["children"]["nextPageStart"]

        return files

    def get_file_content(self, path: str, ref: Optional[str] = None) -> str:
        """Obtiene el contenido raw de un archivo."""
        url = f"{self._api_base}/raw/{path}"
        params = {}
        if ref:
            params["at"] = ref
        resp = self._session.get(url, params=params)
        self._raise_with_detail(resp, "get_file_content")
        return resp.text

    def file_exists(self, path: str, ref: Optional[str] = None) -> bool:
        """Verifica si un archivo existe en el repo."""
        url = f"{self._api_base}/raw/{path}"
        params = {}
        if ref:
            params["at"] = ref
        resp = self._session.get(url, params=params)
        return resp.status_code == 200

    # ── Branches ─────────────────────────────────────────────

    def get_default_branch(self) -> str:
        """Obtiene el nombre de la rama por defecto."""
        url = f"{self._api_base}/default-branch"
        resp = self._session.get(url)
        self._raise_with_detail(resp, "get_default_branch")
        return resp.json().get("displayId", "master")

    def branch_exists(self, branch_name: str) -> bool:
        """Verifica si una rama existe en el repositorio."""
        url = f"{self._api_base}/branches"
        params = {"filterText": branch_name, "limit": 25}
        resp = self._session.get(url, params=params)
        self._raise_with_detail(resp, "branch_exists")
        branches = resp.json().get("values", [])
        return any(b.get("displayId") == branch_name for b in branches)

    def create_branch(self, branch_name: str, start_point: Optional[str] = None) -> dict:
        """
        Crea una nueva rama.

        Args:
            branch_name: Nombre de la rama (ej: 'feature/mesh-ppad-20260305')
            start_point: Rama base (default: rama por defecto del repo)
        """
        if not start_point:
            start_point = self.get_default_branch()

        url = f"{self.base_url}/rest/branch-utils/1.0/projects/{self.project_key}/repos/{self.repo_slug}/branches"
        payload = {
            "name": branch_name,
            "startPoint": f"refs/heads/{start_point}",
        }
        resp = self._session.post(url, json=payload)
        self._raise_with_detail(resp, "create_branch")
        return resp.json()

    def get_latest_commit_id(self, branch: str) -> str:
        """
        Obtiene el ID del último commit en una rama.

        Necesario como sourceCommitId al actualizar archivos existentes
        vía la browse PUT API.
        """
        url = f"{self._api_base}/commits"
        params = {"until": f"refs/heads/{branch}", "limit": 1}
        resp = self._session.get(url, params=params)
        self._raise_with_detail(resp, "get_latest_commit_id")
        values = resp.json().get("values", [])
        if not values:
            raise ValueError(f"No se encontraron commits en la rama '{branch}'")
        return values[0]["id"]

    # ── Commits (via git CLI) ───────────────────────────────

    @property
    def _clone_url(self) -> str:
        """URL HTTPS para git clone/push (sin credenciales)."""
        return (
            f"{self.base_url}/scm"
            f"/{self.project_key}/{self.repo_slug}.git"
        )

    def _git(
        self,
        args: list[str],
        cwd: str,
        env: dict | None = None,
    ) -> subprocess.CompletedProcess:
        """Ejecuta un comando git con auth via http.extraHeader."""
        run_env = (env or os.environ).copy()
        if not self._session.verify:
            run_env["GIT_SSL_NO_VERIFY"] = "1"
        result = subprocess.run(
            ["git", "-c", f"http.extraHeader=Authorization: Bearer {self.token}"]
            + args,
            capture_output=True,
            text=True,
            cwd=cwd,
            env=run_env,
            timeout=120,
        )
        if result.returncode != 0:
            # Ocultar token en mensajes de error
            safe_err = result.stderr.replace(self.token, "***")
            raise RuntimeError(
                f"git {' '.join(args[:3])} falló (rc={result.returncode}): {safe_err}"
            )
        return result

    def commit_file(
        self,
        path: str,
        content: str,
        message: str,
        branch: str,
        source_commit_id: Optional[str] = None,
    ) -> None:
        """
        Crea o actualiza un archivo con un commit vía git push.

        Usa git clone + commit + push porque la API browse PUT está
        deshabilitada en este servidor Bitbucket.

        Args:
            path: Ruta del archivo dentro del repo (ej: 'Local/PPAD/xxx.xml')
            content: Contenido del archivo
            message: Mensaje de commit
            branch: Rama donde hacer el commit
            source_commit_id: (ignorado – se mantiene por compatibilidad)
        """
        with tempfile.TemporaryDirectory(prefix="daia_bb_") as tmpdir:
            repo_dir = os.path.join(tmpdir, "repo")

            # 1. Shallow-clone solo la rama destino
            self._git(
                ["clone", "--depth", "1", "--branch", branch,
                 self._clone_url, repo_dir],
                cwd=tmpdir,
            )

            # 2. Configurar user para el commit
            user_email = os.getenv("BITBUCKET_USER_EMAIL", "")
            user_name = os.getenv("BITBUCKET_USER_NAME", "")
            self._git(["config", "user.email", user_email], cwd=repo_dir)
            self._git(["config", "user.name", user_name], cwd=repo_dir)

            # 3. Escribir el archivo
            file_full = os.path.join(repo_dir, path)
            os.makedirs(os.path.dirname(file_full), exist_ok=True)
            with open(file_full, "w", encoding="utf-8") as f:
                f.write(content)

            # 4. Stage + commit
            self._git(["add", path], cwd=repo_dir)
            self._git(["commit", "-m", message], cwd=repo_dir)

            # 5. Push
            self._git(["push", "origin", branch], cwd=repo_dir)

    # ── Pull Requests ────────────────────────────────────────

    def create_pull_request(
        self,
        title: str,
        source_branch: str,
        target_branch: Optional[str] = None,
        description: str = "",
    ) -> dict:
        """
        Crea un Pull Request.

        Args:
            title: Título del PR
            source_branch: Rama origen
            target_branch: Rama destino (default: rama por defecto)
            description: Descripción del PR

        Returns:
            dict con datos del PR creado (incluye links)
        """
        if not target_branch:
            target_branch = self.get_default_branch()

        url = f"{self._api_base}/pull-requests"
        payload = {
            "title": title,
            "description": description,
            "fromRef": {"id": f"refs/heads/{source_branch}"},
            "toRef": {"id": f"refs/heads/{target_branch}"},
        }
        resp = self._session.post(url, json=payload)
        self._raise_with_detail(resp, "create_pull_request")
        return resp.json()

    def get_pr_url(self, pr_data: dict) -> str:
        """Extrae la URL navegable del PR desde la respuesta de la API."""
        links = pr_data.get("links", {}).get("self", [])
        if links:
            return links[0].get("href", "")
        # Fallback: construir la URL manualmente
        pr_id = pr_data.get("id", "")
        return (
            f"{self.base_url}/projects/{self.project_key}"
            f"/repos/{self.repo_slug}/pull-requests/{pr_id}"
        )
