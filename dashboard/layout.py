"""Shared layout for every dashboard page: one CSS source, nav bar, footer, link builder,
and the small HTML helpers (tables, bars, status tags, timestamps, markdown, diffs).

Self-contained: no external assets, no web fonts, no framework. Pages call `page(...)`.
"""
from __future__ import annotations

import datetime as dt
import re
import urllib.parse
from html import escape as e

PRODUCT = "Pipeline Agent"

# ------------------------------------------------------------------ CSS (the one source)
CSS = """
:root{color-scheme:dark;
--bg:#0f141a;--surface:#161d25;--surface-2:#1c2530;--line:#273240;--ink:#e3e8ee;--ink-2:#b4beca;--muted:#8794a3;
--link:#8bb8f0;--ok:#4cc38a;--warn:#e8b04a;--critical:#ef6b5b;--info:#6aa5f5;
--s1:4px;--s2:8px;--s3:12px;--s4:16px;--s5:24px;--s6:32px;
--t-xs:12px;--t-sm:13px;--t-md:14px;--t-lg:16px;--t-xl:20px;--t-2xl:26px;
--radius:6px;--mono:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
*{box-sizing:border-box}
html,body{margin:0;background:var(--bg);color:var(--ink);font:var(--t-md)/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}
a{color:var(--link);text-decoration:none}a:hover{text-decoration:underline}
code{font:var(--t-xs) var(--mono);overflow-wrap:break-word}
h1{font-size:var(--t-xl);margin:0 0 var(--s1)}h2{font-size:var(--t-lg);margin:0 0 var(--s3)}
h3{font-size:var(--t-md);margin:var(--s4) 0 var(--s2);color:var(--ink-2)}
.top{background:var(--surface);border-bottom:1px solid var(--line)}
.top .in,main,.foot{max-width:1240px;margin:0 auto;padding-left:var(--s5);padding-right:var(--s5)}
.top .in{display:flex;align-items:center;gap:var(--s6);height:52px}
.brand{font-weight:600;font-size:var(--t-lg);color:var(--ink)}
.nav{display:flex;gap:var(--s1);flex-wrap:wrap}
.nav a{color:var(--ink-2);padding:var(--s1) var(--s3);border-radius:var(--radius)}
.nav a:hover{background:var(--surface-2);text-decoration:none}
.nav a[aria-current]{color:var(--ink);background:var(--surface-2);box-shadow:inset 0 -2px 0 var(--info)}
main{padding-top:var(--s5);padding-bottom:var(--s5)}
.head{margin-bottom:var(--s5)}.sub{color:var(--muted);margin:0 0 var(--s4)}
.foot{color:var(--muted);font-size:var(--t-xs);padding-top:var(--s4);padding-bottom:var(--s6);border-top:1px solid var(--line)}
.card{background:var(--surface);border:1px solid var(--line);border-radius:var(--radius);padding:var(--s4);margin-bottom:var(--s4);min-width:0}
.card>h2 .more{float:right;font-size:var(--t-sm);font-weight:400}
.grid{display:grid;gap:var(--s3)}.two{grid-template-columns:repeat(auto-fit,minmax(320px,1fr))}
.tiles{grid-template-columns:repeat(6,minmax(0,1fr));margin-bottom:var(--s4)}
@media (max-width:900px){.tiles{grid-template-columns:repeat(3,minmax(0,1fr))}}
.tile{display:block;background:var(--surface);border:1px solid var(--line);border-radius:var(--radius);padding:var(--s3) var(--s4);color:var(--ink)}
.tile:hover{border-color:var(--ink-2);text-decoration:none}
.tile .n{font-size:var(--t-2xl);font-weight:600;font-variant-numeric:tabular-nums}.tile .l{color:var(--muted);font-size:var(--t-xs)}
.wrap{overflow-x:auto;max-width:100%}
table{border-collapse:collapse;width:100%;font-size:var(--t-sm)}
th,td{text-align:left;padding:var(--s1) var(--s2);border-bottom:1px solid var(--line);vertical-align:top}
th{color:var(--muted);font-weight:600;font-size:var(--t-xs);white-space:nowrap}
tr:last-child td{border-bottom:0}.num{text-align:right;font-variant-numeric:tabular-nums}.nowrap{white-space:nowrap}
dl.kv{display:grid;grid-template-columns:max-content 1fr;gap:var(--s1) var(--s4);margin:0}dl.kv dt{color:var(--muted)}dl.kv dd{margin:0}
.ok{color:var(--ok)}.warn{color:var(--warn)}.critical{color:var(--critical)}.info{color:var(--info)}.muted{color:var(--muted)}
.tag{display:inline-block;padding:0 var(--s2);border-radius:10px;font-size:var(--t-xs);border:1px solid currentColor;white-space:nowrap;line-height:18px}
.empty{color:var(--muted);padding:var(--s3) 0;margin:0}
.chips{display:flex;flex-wrap:wrap;gap:var(--s2);align-items:center;margin:0 0 var(--s3)}
.chip{display:inline-block;padding:var(--s1) var(--s3);border:1px solid var(--line);border-radius:14px;color:var(--ink-2);font-size:var(--t-sm)}
.chip[aria-current]{border-color:var(--info);color:var(--ink)}
.bar{display:flex;align-items:center;gap:var(--s2)}.bar i{display:block;height:8px;background:var(--muted);border-radius:3px;min-width:2px}
.bar i.critical{background:var(--critical)}.bar i.warn{background:var(--warn)}.bar i.info{background:var(--info)}
.notice{border-left:3px solid var(--info)}.notice.err{border-left-color:var(--critical)}.notice.warn{border-left-color:var(--warn)}
.ready{border-color:var(--warn)}.ready>h2{color:var(--warn)}
pre.diff{background:var(--bg);border:1px solid var(--line);border-radius:var(--radius);padding:var(--s2) var(--s3);overflow-x:auto;font:var(--t-xs)/1.45 var(--mono);margin:var(--s2) 0}
pre.diff .add{color:var(--ok)}pre.diff .del{color:var(--critical)}pre.diff .hunk{color:var(--info)}
.actions{display:flex;gap:var(--s4);align-items:flex-start;flex-wrap:wrap;margin-top:var(--s4);padding-top:var(--s4);border-top:1px solid var(--line)}
.actions form{margin:0;display:flex;gap:var(--s2);align-items:flex-start}
button,.btn{font:inherit;font-size:var(--t-sm);border:1px solid transparent;border-radius:var(--radius);padding:6px var(--s4);cursor:pointer;color:#fff;display:inline-block;list-style:none}
.btn::-webkit-details-marker{display:none}
.approve{background:#23734f}.approve:hover{background:#2a8a5f}.reject{background:#9b3a30}.reject:hover{background:#b04236}
.secondary{background:transparent;border-color:var(--line);color:var(--ink-2)}
details.confirm[open]>summary{background:transparent;border-color:var(--line);color:var(--ink-2)}
details.confirm[open]>summary::after{content:" (cancel)"}
.confirm-box{margin-top:var(--s3);padding:var(--s3);border:1px solid var(--warn);border-radius:var(--radius);max-width:560px}
.confirm-box p{margin:0 0 var(--s2)}
textarea,select,input[type=text],input[type=date]{font:inherit;font-size:var(--t-sm);border:1px solid var(--line);border-radius:var(--radius);padding:5px var(--s2);background:var(--bg);color:var(--ink)}
textarea{width:320px;max-width:100%;height:34px;resize:vertical}
form.filters{display:flex;flex-wrap:wrap;gap:var(--s3);align-items:flex-end;margin:0 0 var(--s3)}
form.filters label{display:flex;flex-direction:column;gap:2px;font-size:var(--t-xs);color:var(--muted)}
details{margin:var(--s2) 0}summary{cursor:pointer}details.card>summary{font-weight:600}details>summary+*{margin-top:var(--s2)}
.md h1,.md h2,.md h3,.md h4{font-size:var(--t-md);margin:var(--s3) 0 var(--s1)}.md p,.md li{margin:2px 0}.md ul{padding-left:20px;margin:var(--s1) 0}
.audit table{font-size:var(--t-xs)}.audit code.pid{white-space:nowrap}
.audit td:last-child{min-width:180px;max-width:320px;overflow-wrap:anywhere}
.rules li{margin:3px 0}.stat{display:inline-block;margin:0 var(--s5) var(--s1) 0}.stat b{font-size:var(--t-lg)}
"""

NAV = [("overview", "Overview"), ("pipelines", "Pipelines"), ("incidents", "Incidents"),
       ("proposals", "Proposals"), ("governance", "Governance")]


# ------------------------------------------------------------------ links
def static_name(s: str) -> str:
    """Filename-safe form of an id for static pages."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", s)


class Links:
    """Route URLs for the live server, or relative file names for the static build.
    A static link is None when the target has no static page (callers then omit it)."""

    def __init__(self, live: bool, pipelines: set | None = None):
        self.live = live
        self.known = pipelines          # when given, links to unknown pipeline ids are omitted

    def overview(self) -> str:
        return "/" if self.live else "index.html"

    def pipelines(self) -> str:
        return "/pipelines" if self.live else "pipelines.html"

    def pipeline(self, pid: str) -> str | None:
        if self.known is not None and pid not in self.known:
            return None
        return f"/pipeline/{urllib.parse.quote(pid)}" if self.live else f"pipeline-{static_name(pid)}.html"

    def incidents(self, **filters) -> str | None:
        f = {k: v for k, v in filters.items() if v}
        if self.live:
            return "/incidents" + (f"?{urllib.parse.urlencode(f)}" if f else "")
        if not f:
            return "incidents.html"
        if list(f) == ["severity"]:
            return f"incidents-{static_name(f['severity'])}.html"
        return None

    def proposals(self, status: str = "") -> str:
        if self.live:
            return "/proposals" + (f"?{urllib.parse.urlencode({'status': status})}" if status else "")
        return f"proposals-{static_name(status)}.html" if status else "proposals.html"

    def governance(self, pid: str = "") -> str:
        anchor = f"#contract-{static_name(pid)}" if pid else ""
        if self.live:
            return "/governance" + (f"?{urllib.parse.urlencode({'open': pid})}" if pid else "") + anchor
        return "governance.html" + anchor

    def nav(self, key: str) -> str:
        return {"overview": self.overview, "pipelines": self.pipelines, "incidents": self.incidents,
                "proposals": self.proposals, "governance": self.governance}[key]()


# ------------------------------------------------------------------ helpers
class Raw(str):
    """Marks a string as already-safe HTML."""


def h(x) -> str:
    """Escape unless already Raw."""
    return x if isinstance(x, Raw) else e(str(x))


def _cls(cls: str) -> str:
    return f' class="{e(cls)}"' if cls else ""


def code(x, cls: str = "") -> Raw:
    return Raw(f"<code{_cls(cls)}>{e(str(x))}</code>")


def a(href: str | None, text, cls: str = "") -> Raw:
    """Link, or plain text when href is None."""
    body = h(text)
    if not href:
        return Raw(body)
    return Raw(f'<a href="{e(href)}"{_cls(cls)}>{body}</a>')


_TS = re.compile(r"^(\d{4}-\d\d-\d\d)[T ](\d\d:\d\d)")


def fmt_ts(ts) -> str:
    """'2026-11-28T02:30:41Z' -> '2026-11-28 02:30'; anything else (dates, 'not recorded') unchanged."""
    if isinstance(ts, dt.datetime):
        return ts.strftime("%Y-%m-%d %H:%M")
    m = _TS.match(str(ts or ""))
    return f"{m.group(1)} {m.group(2)}" if m else ("" if ts is None else str(ts))


def fmt_int(n) -> str:
    return "-" if n is None else f"{n:,}"


STATUS_CLASS = {"APPLIED": "ok", "READY_FOR_APPROVAL": "warn", "ESCALATED": "info", "REJECTED": "muted",
                "FAILED": "critical", "FAILED_VALIDATION": "critical",
                "critical": "critical", "warn": "warn", "info": "info",
                "open": "warn", "resolved_pending_verification": "ok", "resolved": "ok", "closed": "muted"}


def status_class(s: str) -> str:
    return STATUS_CLASS.get(s, "muted")


def tag(s: str, cls: str | None = None) -> Raw:
    return Raw(f'<span class="tag {cls or status_class(s)}">{e(str(s))}</span>')


def sev(s: str) -> Raw:
    return Raw(f'<span class="{status_class(s)}">{e(str(s))}</span>')


def table(headers: list, rows: list, num: set = frozenset(), empty: str = "Nothing to show.",
          cls: str = "") -> str:
    if not rows:
        return f'<p class="empty">{e(empty)}</p>'
    th = "".join(f"<th{' class=num' if i in num else ''}>{e(x)}</th>" for i, x in enumerate(headers))
    body = "".join("<tr>" + "".join(f"<td{' class=num' if i in num else ''}>{h(c)}</td>"
                                    for i, c in enumerate(r)) + "</tr>" for r in rows)
    return f'<div class="wrap{" " + cls if cls else ""}"><table><tr>{th}</tr>{body}</table></div>'


def bars(counts: dict, href=None, empty: str = "None.") -> str:
    """Horizontal count bars; href(key) -> link for that row (or None)."""
    if not counts:
        return f'<p class="empty">{e(empty)}</p>'
    top = max(counts.values()) or 1
    rows = [[a(href(k) if href else None, k),
             Raw(f'<div class="bar"><i class="{status_class(k) if k in ("critical", "warn", "info") else ""}" '
                 f'style="width:{max(2, int(180 * v / top))}px"></i>{v}</div>')]
            for k, v in counts.items()]
    return table(["", ""], rows)


_TS_INLINE = re.compile(r"\b(\d{4}-\d\d-\d\d)T(\d\d:\d\d)(?::\d\d(?:\.\d+)?)?(?:Z|[+-]\d\d:?\d\d)?\b")


def _inline(text: str) -> str:
    text = _TS_INLINE.sub(r"\1 \2", e(text))
    text = re.sub(r"`([^`]+)`", r"<code>\1</code>", text)
    return re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", text)


def markdown_to_html(md: str) -> str:
    """Minimal markdown: #-headings, pipe tables, '-' bullets, paragraphs, **bold**, `code`."""
    out, lines, i = [], md.splitlines(), 0
    in_list = False
    while i < len(lines):
        ln = lines[i]
        if in_list and not ln.startswith("- "):
            out.append("</ul>")
            in_list = False
        if ln.startswith("|") and i + 1 < len(lines) and re.match(r"^\|[\s:|-]+\|$", lines[i + 1]):
            cells = lambda s: [c.strip() for c in s.strip().strip("|").split("|")]  # noqa: E731
            head, i, rows = cells(ln), i + 2, []
            while i < len(lines) and lines[i].startswith("|"):
                rows.append(cells(lines[i]))
                i += 1
            out.append('<div class="wrap"><table><tr>' + "".join(f"<th>{_inline(x)}</th>" for x in head)
                       + "</tr>" + "".join("<tr>" + "".join(f"<td>{_inline(c)}</td>" for c in r) + "</tr>"
                                           for r in rows) + "</table></div>")
            continue
        m = re.match(r"^(#{1,6})\s+(.*)", ln)
        if m:
            lvl = min(len(m.group(1)) + 1, 4)
            out.append(f"<h{lvl}>{_inline(m.group(2))}</h{lvl}>")
        elif ln.startswith("- "):
            if not in_list:
                out.append("<ul>")
                in_list = True
            out.append(f"<li>{_inline(ln[2:])}</li>")
        elif ln.strip():
            out.append(f"<p>{_inline(ln.strip())}</p>")
        i += 1
    if in_list:
        out.append("</ul>")
    return "\n".join(out)


def diff_html(diff: str) -> str:
    out = []
    for ln in diff.splitlines():
        cls = ("add" if ln.startswith("+") and not ln.startswith("+++") else
               "del" if ln.startswith("-") and not ln.startswith("---") else
               "hunk" if ln.startswith("@@") else "")
        out.append(f'<span class="{cls}">{e(ln)}</span>' if cls else e(ln))
    return '<pre class="diff">' + "\n".join(out) + "</pre>"


def diff_files(diff: str) -> list[str]:
    """Paths a unified diff touches (from its +++/--- headers)."""
    files = []
    for ln in diff.splitlines():
        if ln.startswith(("+++ ", "--- ")):
            p = ln[4:].split("\t")[0].strip()
            if p != "/dev/null":
                p = re.sub(r"^[ab]/", "", p)
                if p not in files:
                    files.append(p)
    return files


# ------------------------------------------------------------------ page shell
def page(title: str, active: str, body: str, links: Links, now: dt.datetime | None = None,
         head_extra: str = "", foot_note: str = "") -> str:
    """The one base layout: top bar with product name + nav, <main>, footer with generated-at."""
    now = now or dt.datetime.now()
    nav = "".join(f'<a href="{e(links.nav(k))}"{" aria-current=page" if k == active else ""}>{e(label)}</a>'
                  for k, label in NAV)
    full_title = PRODUCT if active == "overview" and title == PRODUCT else f"{title} \u00b7 {PRODUCT}"
    return ("<!doctype html><html lang=en><head><meta charset=utf-8>"
            "<meta name=viewport content='width=device-width,initial-scale=1'>"
            f"<title>{e(full_title)}</title><style>{CSS}</style>{head_extra}</head><body>"
            f'<header class="top"><div class="in"><a class="brand" href="{e(links.overview())}">{PRODUCT}</a>'
            f'<nav class="nav" aria-label="Main">{nav}</nav></div></header>'
            f"<main>{body}</main>"
            f'<footer class="foot">Generated <span id="generated-at">{e(fmt_ts(now))}</span>'
            " &middot; read-only view of manifests, detection/incidents.db, healing/proposals and evaluation results"
            f"{' &middot; ' + foot_note if foot_note else ''}</footer></body></html>")


def heading(title: str, sub: str = "") -> str:
    return f'<div class="head"><h1>{e(title)}</h1>{f"<p class=sub>{sub}</p>" if sub else ""}</div>'


def not_found(message: str, links: Links, now: dt.datetime | None = None) -> str:
    body = (heading("Not found") + f'<section class="card"><p class="empty">{e(message)}</p>'
            f'<p><a href="{e(links.overview())}">Back to overview</a></p></section>')
    return page("Not found", "", body, links, now)
