"""Credential-safety tests for the standalone ANWB API probe."""

from __future__ import annotations

import argparse
import io
import json
import os
import stat
from datetime import date
from pathlib import Path
from urllib.error import HTTPError

import pytest

from scripts import anwb_api_probe as probe

SENTINEL_SECRET = "sentinel-oauth-secret-must-not-appear"


def test_callback_is_requested_without_a_command_line_value(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    parser = probe.build_parser()
    args = parser.parse_args(["probe", "--callback"])

    assert args.prompt_for_callback is True
    assert not hasattr(args, "callback_url")

    callback_url = (
        f"https://login.anwb.nl/example/callback?code={SENTINEL_SECRET}"
    )
    prompts: list[str] = []

    def fake_getpass(prompt: str) -> str:
        prompts.append(prompt)
        return callback_url

    monkeypatch.setattr(probe.getpass, "getpass", fake_getpass)

    assert probe._prompt_callback_url() == callback_url
    captured = capsys.readouterr()
    assert SENTINEL_SECRET not in captured.out
    assert SENTINEL_SECRET not in captured.err
    assert SENTINEL_SECRET not in prompts[0]


def test_callback_url_argument_is_rejected_without_being_echoed(
    capsys: pytest.CaptureFixture[str],
) -> None:
    callback_url = (
        f"https://login.anwb.nl/example/callback?code={SENTINEL_SECRET}"
    )

    with pytest.raises(SystemExit) as error:
        probe.build_parser().parse_args(["probe", callback_url])

    assert error.value.code == 2
    captured = capsys.readouterr()
    assert SENTINEL_SECRET not in captured.out
    assert SENTINEL_SECRET not in captured.err


def test_http_error_body_is_discarded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def raise_http_error(_request: object, *, timeout: int) -> None:
        assert timeout == 30
        body = io.BytesIO(
            json.dumps({"access_token": SENTINEL_SECRET}).encode("utf-8")
        )
        raise HTTPError(
            "https://api.example.invalid/token",
            401,
            "Unauthorized",
            hdrs=None,
            fp=body,
        )

    monkeypatch.setattr(probe, "urlopen", raise_http_error)

    response = probe._request_json(
        "POST",
        "https://api.example.invalid/token",
    )

    assert response.status == 401
    assert response.payload == {}
    assert response.is_json_object is False
    assert SENTINEL_SECRET not in repr(response)


def test_token_exchange_error_exposes_status_but_not_response_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        probe,
        "_read_json",
        lambda _path: {"code_verifier": "verifier", "state": "expected"},
    )
    monkeypatch.setattr(
        probe,
        "_request_json",
        lambda *_args, **_kwargs: probe.JsonResponse(
            status=401,
            payload={"error_description": SENTINEL_SECRET},
            is_json_object=True,
        ),
    )

    with pytest.raises(probe.ProbeError) as error:
        probe._exchange_callback(
            f"{probe.REDIRECT_URI}?code=fake&state=expected"
        )

    message = str(error.value)
    assert message == "OAuth token exchange failed with HTTP 401"
    assert SENTINEL_SECRET not in message


@pytest.mark.parametrize(
    "callback_url",
    [
        f"{probe.REDIRECT_URI}?code={SENTINEL_SECRET}",
        f"{probe.REDIRECT_URI}?code=fake&state=",
    ],
)
def test_callback_requires_one_non_empty_state(
    callback_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        probe,
        "_read_json",
        lambda _path: {"code_verifier": "verifier", "state": "expected"},
    )

    with pytest.raises(probe.ProbeError) as error:
        probe._exchange_callback(callback_url)

    assert "exactly one non-empty state" in str(error.value)
    assert SENTINEL_SECRET not in str(error.value)


def test_callback_state_must_match_without_exposing_either_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    saved_state = "saved-state-sensitive-value"
    callback_state = f"callback-{SENTINEL_SECRET}"
    monkeypatch.setattr(
        probe,
        "_read_json",
        lambda _path: {"code_verifier": "verifier", "state": saved_state},
    )

    with pytest.raises(probe.ProbeError) as error:
        probe._exchange_callback(
            f"{probe.REDIRECT_URI}?code=fake&state={callback_state}"
        )

    message = str(error.value)
    assert message == "Callback state does not match the saved login state"
    assert saved_state not in message
    assert callback_state not in message
    assert SENTINEL_SECRET not in message


@pytest.mark.parametrize(
    "callback_url",
    [
        (
            f"http://login.anwb.nl/{probe.TENANT_ID}/login/callback"
            f"?code={SENTINEL_SECRET}&state=expected"
        ),
        (
            f"https://foreign.example/{probe.TENANT_ID}/login/callback"
            f"?code={SENTINEL_SECRET}&state=expected"
        ),
        (
            f"https://login.anwb.nl:444/{probe.TENANT_ID}/login/callback"
            f"?code={SENTINEL_SECRET}&state=expected"
        ),
        (
            f"https://login.anwb.nl:0/{probe.TENANT_ID}/login/callback"
            f"?code={SENTINEL_SECRET}&state=expected"
        ),
        (
            f"https://login.anwb.nl/{probe.TENANT_ID}/login/other"
            f"?code={SENTINEL_SECRET}&state=expected"
        ),
        (
            f"https://user:password@login.anwb.nl/{probe.TENANT_ID}/login/callback"
            f"?code={SENTINEL_SECRET}&state=expected"
        ),
    ],
)
def test_callback_rejects_foreign_redirect_components(
    callback_url: str,
) -> None:
    with pytest.raises(probe.ProbeError) as error:
        probe._extract_code(callback_url)

    message = str(error.value)
    assert message == "Callback URL does not match the configured redirect URI"
    assert SENTINEL_SECRET not in message


@pytest.mark.parametrize(
    ("query", "expected_error"),
    [
        (
            f"code=first&code={SENTINEL_SECRET}&state=expected",
            "exactly one non-empty code",
        ),
        (
            f"code=fake&state=first&state={SENTINEL_SECRET}",
            "exactly one non-empty state",
        ),
    ],
)
def test_callback_rejects_duplicate_code_or_state(
    query: str,
    expected_error: str,
) -> None:
    with pytest.raises(probe.ProbeError) as error:
        probe._extract_code(f"{probe.REDIRECT_URI}?{query}")

    message = str(error.value)
    assert expected_error in message
    assert SENTINEL_SECRET not in message


def test_callback_accepts_explicit_default_https_port() -> None:
    callback_url = probe.REDIRECT_URI.replace(
        "https://login.anwb.nl",
        "https://login.anwb.nl:443",
    )

    assert probe._extract_code(
        f"{callback_url}?code=fake&state=expected"
    ) == ("fake", "expected")


def test_secret_json_is_created_with_restrictive_permissions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probe_dir = tmp_path / "probe"
    secret_file = probe_dir / "tokens.json"
    probe_dir.mkdir()
    secret_file.touch(mode=0o644)
    os.chmod(secret_file, 0o644)
    monkeypatch.setattr(probe, "PROBE_DIR", probe_dir)

    probe._write_secret_json(
        secret_file,
        {"access_token": SENTINEL_SECRET},
    )

    assert stat.S_IMODE(secret_file.stat().st_mode) == 0o600
    assert json.loads(secret_file.read_text(encoding="utf-8")) == {
        "access_token": SENTINEL_SECRET
    }


def test_secret_json_does_not_follow_a_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if not getattr(os, "O_NOFOLLOW", 0):
        pytest.skip("O_NOFOLLOW is unavailable on this platform")

    probe_dir = tmp_path / "probe"
    probe_dir.mkdir()
    target = tmp_path / "target.json"
    target.write_text("unchanged", encoding="utf-8")
    secret_link = probe_dir / "tokens.json"
    secret_link.symlink_to(target)
    monkeypatch.setattr(probe, "PROBE_DIR", probe_dir)

    with pytest.raises(OSError):
        probe._write_secret_json(
            secret_link,
            {"access_token": SENTINEL_SECRET},
        )

    assert target.read_text(encoding="utf-8") == "unchanged"


def test_probe_report_keeps_only_allowlisted_error_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    probe_dir = tmp_path / "probe"
    report_file = probe_dir / "last-report.json"
    monkeypatch.setattr(probe, "PROBE_DIR", probe_dir)
    monkeypatch.setattr(probe, "REPORT_FILE", report_file)
    monkeypatch.setattr(probe, "TOKEN_FILE", probe_dir / "tokens.json")
    monkeypatch.setattr(probe, "_get_oauth_token", lambda _callback: "oauth")
    monkeypatch.setattr(probe, "_get_kraken_token", lambda _token: "kraken")
    monkeypatch.setattr(probe, "_get_account_number", lambda _token: "account")
    monkeypatch.setattr(
        probe,
        "_probe_windows",
        lambda _previous_year: [
            probe.ProbeWindow(
                name="test_window",
                start=date(2026, 1, 1),
                end=date(2026, 1, 1),
                intervals=("HOUR",),
            )
        ],
    )
    monkeypatch.setattr(
        probe,
        "_fetch_cache",
        lambda *_args: probe.JsonResponse(
            status=503,
            payload={"error_description": SENTINEL_SECRET},
            is_json_object=True,
        ),
    )
    args = argparse.Namespace(
        prompt_for_callback=False,
        previous_year=False,
        tariff_check=False,
    )

    assert probe.command_probe(args) == 0

    report_text = report_file.read_text(encoding="utf-8")
    captured = capsys.readouterr()
    assert SENTINEL_SECRET not in report_text
    assert SENTINEL_SECRET not in captured.out
    assert SENTINEL_SECRET not in captured.err

    report = json.loads(report_text)
    assert report["classification"]["request_failures"] == 3
    for result in report["results"]:
        assert result == {
            "endpoint": result["endpoint"],
            "http_status": 503,
            "interval": "HOUR",
            "outcome": "http_error",
            "window": "test_window",
        }


def test_unexpected_error_details_are_not_printed(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fail(_args: argparse.Namespace) -> int:
        raise RuntimeError(SENTINEL_SECRET)

    class FakeParser:
        @staticmethod
        def parse_args() -> argparse.Namespace:
            return argparse.Namespace(func=fail)

    monkeypatch.setattr(probe, "build_parser", FakeParser)

    assert probe.main() == 1
    captured = capsys.readouterr()
    assert SENTINEL_SECRET not in captured.out
    assert SENTINEL_SECRET not in captured.err
    assert "response details were not exposed" in captured.err
