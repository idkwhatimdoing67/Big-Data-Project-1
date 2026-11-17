"""Save screenshots of the actual Kibana dashboard at elapsed intervals."""

import argparse
from datetime import datetime
import os
from pathlib import Path
import time

from playwright.sync_api import Error, sync_playwright

# Wait for a mark in this project's Vega panel, so an empty chart or loading
# screen does not produce a submission screenshot.
RENDERED_BARS = """() => [...document.querySelectorAll('canvas')].some(canvas => {
    const context = canvas.getContext('2d');
    if (!context || canvas.width < 50 || canvas.height < 50) return false;
    const {data} = context.getImageData(0, 0, canvas.width, canvas.height);
    for (let i = 0; i < data.length; i += 4) {
      if (data[i] === 0 && data[i+1] === 119 && data[i+2] === 168 && data[i+3] === 255) {
        return true;
      }
    }
    return false;
})"""


def capture(page, url, path):
    page.goto(url, wait_until="domcontentloaded", timeout=30_000)
    page.get_by_text("Top 10 named entities", exact=True).wait_for(timeout=30_000)
    for name in ("Dismiss toast", "Dismiss"):
        button = page.get_by_role("button", name=name, exact=True).first
        if button.is_visible():
            button.click()
    page.wait_for_function(RENDERED_BARS, timeout=30_000)
    temporary = path.with_suffix(".tmp.png")
    page.screenshot(path=temporary)
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    port = os.environ.get("KIBANA_HOST_PORT", "5601")
    parser.add_argument("--url", default=f"http://127.0.0.1:{port}/app/dashboards#/view/cs4371-entities")
    parser.add_argument("--output", type=Path, default=Path("outputs/entity_charts"))
    parser.add_argument("--at-seconds", type=int, nargs="+", default=[900, 1800, 2700, 3600])
    args = parser.parse_args()
    if args.at_seconds != sorted(set(args.at_seconds)) or args.at_seconds[0] < 0:
        parser.error("--at-seconds must list distinct nonnegative times in increasing order.")

    args.output.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    stamp = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
    failures = []
    with sync_playwright() as playwright:
        for elapsed in args.at_seconds:
            time.sleep(max(0, started + elapsed - time.monotonic()))
            label = f"{elapsed // 60:02d}min" if elapsed % 60 == 0 else f"{elapsed:04d}s"
            path = args.output / f"timed-{stamp}-{label}.png"
            try:
                # Start Chrome only while capturing, with a fresh temporary profile.
                browser = playwright.chromium.launch(channel="chrome")
                try:
                    page = browser.new_page(viewport={"width": 1360, "height": 920})
                    capture(page, args.url, path)
                finally:
                    browser.close()
            except Error as exc:
                failures.append(label)
                print(f"Capture at {label} failed: {exc}", flush=True)
                continue
            print(f"Saved {path} at {datetime.now().astimezone().isoformat(timespec='seconds')} "
                  f"(elapsed {time.monotonic() - started:.1f}s)", flush=True)
    if failures:
        raise SystemExit(f"Failed capture times: {', '.join(failures)}")


if __name__ == "__main__":
    main()
