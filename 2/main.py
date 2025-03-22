import os
import asyncio
import time
from dataclasses import dataclass
from typing import Final, Any, List
from dotenv import load_dotenv
from aiohttp import ClientSession

GITHUB_API_BASE_URL: Final[str] = "https://api.github.com"
load_dotenv()


@dataclass
class RepositoryAuthorCommitsNum:
    author: str
    commits_num: int


@dataclass
class Repository:
    name: str
    owner: str
    position: int
    stars: int
    watchers: int
    forks: int
    language: str
    authors_commits_num_today: List[RepositoryAuthorCommitsNum]


class GithubReposScrapper:
    def __init__(self, access_token: str):
        self._session = ClientSession(
            headers={
                "Accept": "application/vnd.github.v3+json",
                "Authorization": f"Bearer {access_token}",
            }
        )
        self._rate_limiter = None

    async def _make_request(
            self, endpoint: str, method: str = "GET", params: dict[str, Any] | None = None
    ) -> Any:
        try:
            async with self._session.request(
                    method, f"{GITHUB_API_BASE_URL}/{endpoint}", params=params
            ) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            return {}

    async def _get_top_repositories(self, limit: int = 100) -> List[dict[str, Any]]:
        """
        GitHub REST API:
        https://docs.github.com/en/rest/search/search?apiVersion=2022-11-28#search-repositories
        """
        data = await self._make_request(
            endpoint="search/repositories",
            params={"q": "stars:>1", "sort": "stars", "order": "desc", "per_page": limit},
        )
        return data.get("items", [])

    async def _get_repository_commits(self, owner: str, repo: str) -> list[dict[str, Any]]:
        """GitHub REST API: https://docs.github.com/en/rest/commits/commits?apiVersion=2022-11-28#list-commits"""
        ...

    async def get_repositories(self) -> List[Repository]:
        try:
            mcr = int(os.getenv("GITHUB_MCR", "5"))
        except ValueError:
            mcr = 5
        try:
            rps = float(os.getenv("GITHUB_RPS", "1.0"))
        except ValueError:
            rps = 1.0

        class RateLimiter:
            def __init__(self, rate: float):
                self._rate = rate
                self._min_interval = 1.0 / rate
                self._lock = asyncio.Lock()
                self._last_call = 0.0

            async def wait(self):
                async with self._lock:
                    now = time.monotonic()
                    elapsed = now - self._last_call
                    wait_time = self._min_interval - elapsed
                    if wait_time > 0:
                        await asyncio.sleep(wait_time)
                    self._last_call = time.monotonic()

        self._rate_limiter = RateLimiter(rps)

        semaphore = asyncio.Semaphore(mcr)

        top_repos_data = await self._get_top_repositories()

        async def process_repo(index: int, repo_data: dict[str, Any]) -> Repository:
            owner = repo_data["owner"]["login"]
            repo_name = repo_data["name"]
            async with semaphore:
                commits = await self._get_repository_commits(owner, repo_name)
            author_counts: dict[str, int] = {}
            for commit in commits:
                try:
                    if commit.get("author") and commit["author"].get("login"):
                        author = commit["author"]["login"]
                    else:
                        author = commit["commit"]["author"]["name"]
                    author_counts[author] = author_counts.get(author, 0) + 1
                except Exception:
                    continue
            authors_commits = [
                RepositoryAuthorCommitsNum(author=a, commits_num=c)
                for a, c in author_counts.items()
            ]
            return Repository(
                name=repo_data["name"],
                owner=owner,
                position=index + 1,
                stars=repo_data.get("stargazers_count", 0),
                watchers=repo_data.get("watchers_count", 0),
                forks=repo_data.get("forks_count", 0),
                language=repo_data.get("language") or "Unknown",
                authors_commits_num_today=authors_commits,
            )

        tasks = [process_repo(i, repo) for i, repo in enumerate(top_repos_data)]
        try:
            results = await asyncio.gather(*tasks, return_exceptions=False)
        except Exception as e:
            results = []
        return results

    async def close(self):
        await self._session.close()
