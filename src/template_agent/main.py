from __future__ import annotations

import argparse
from pathlib import Path
from rich.console import Console
from rich.table import Table

from .agent import TemplateAgent
from .schemas import TemplateInput
from .settings import get_settings
from .utils.io import read_json, write_json

console = Console()

def _print_summary(output: dict):
    table = Table(title="Template Agent Output Summary")
    table.add_column("Field")
    table.add_column("Value", overflow="fold")

    table.add_row("campaign_goal", str(output.get("campaign_goal")))
    table.add_row("channel", str(output.get("channel")))
    table.add_row("step_id", str(output.get("step_id")))
    table.add_row("persona_id", str(output.get("persona_id")))
    table.add_row("tone_id", str(output.get("tone_id")))
    # table.add_row("candidate_count", str(output.get("candidate_count", [])))
    cands = output.get("candidates") or []
    table.add_row("candidate_count", str(len(cands)))
    table.add_row("warnings", "\n".join(output.get("warnings", []))[:600])
    console.print(table)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="입력 JSON 파일 경로 (test/fixture/sample_inputs.json 등)")
    parser.add_argument("--index", type=int, default=0, help="입력 JSON이 리스트일 때 사용할 인덱스")
    parser.add_argument("--output", default="output.json", help="출력 JSON 저장 경로")
    args = parser.parse_args()

    s = get_settings()

    agent = TemplateAgent(
        model=s.model,
        temperature = s.temperature,
        max_output_tokens= s.max_output_tokens,
        candidate_count = s.candidate_count,
    )

    data = read_json(args.input)
    if isinstance(data, list):
        item = data[args.index]
    else:
        item = data

    inp = TemplateInput.model_validate(item)
    out = agent.run(inp)

    out_dict = out.model_dump()
    write_json(args.output, out_dict)

    console.print(f"[green]Saved:[/green] {Path(args.output).resolve()}")
    _print_summary(out_dict)

if __name__ == "__main__":
        main()