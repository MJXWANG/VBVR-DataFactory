# Project Guidelines

## Overview

**VM Data Wheel** — Serverless data generation for video reasoning models.

- One-click deploy via CloudFormation (no local setup required for users)
- Developers use CDK for infrastructure changes
- Lambda generates samples from SQS tasks, outputs to S3

## Architecture

```
CloudFormation → Submit Lambda → SQS Queue → Generator Lambda → S3 Bucket
                                    ↓
                                   DLQ (failed tasks)
```

**Key Resources:**
- `{stack-name}-submit-tasks` — Lambda to batch-submit tasks
- `{stack-name}-queue` — SQS task queue
- `{stack-name}-generator` — Lambda with 50+ generators (Docker image)
- `{stack-name}-output-{account-id}` — S3 output bucket

## Tech Stack

- Python 3.11+
- UV for package management
- AWS CDK for infrastructure
- pytest for testing
- Ruff for linting/formatting

## Commands

**Always use `uv run`** (no system Python):

```bash
uv sync --extra dev --extra cdk     # Install dependencies
uv run pytest                        # Run tests
uv run ruff check src/ scripts/      # Lint
uv run ruff format src/ scripts/     # Format
uv run cdk deploy                    # Deploy (set AWS_PROFILE first)
```

## Code Style

- Line length: 120 characters
- Type hints for function signatures
- f-strings for formatting
- `logging` module in `src/` (not `print()`)
- All code and docs in English

## Project Structure

```
src/              # Lambda source (handler, generator, uploader)
cdk/              # CDK infrastructure
cloudformation/   # One-click deploy template (generated from CDK)
scripts/          # CLI utilities
  static/         # Web UI files
tests/            # pytest tests
generators/       # Generator repos (gitignored)
```

## Key Files

| File | Purpose |
|------|---------|
| `cloudformation/VmDatasetPipelineStack.template.json` | One-click deploy template |
| `cdk/stacks/pipeline_stack.py` | CDK stack definition |
| `src/handler.py` | Lambda entry point |
| `src/generator.py` | Generator execution logic |
| `scripts/generator_config.json` | Per-generator batch size config |
| `requirements-all.txt` | Merged generator dependencies |

## Dependencies

- `pyproject.toml` — Pipeline dependencies
- `requirements-all.txt` — All generator dependencies (for Docker image)

Run `scripts/collect_requirements.sh` to update `requirements-all.txt` when generators change.

## Testing

```bash
uv run pytest                              # Unit tests
uv run python scripts/test_server.py       # Web UI at :8000
uv run python scripts/local_test.py        # CLI testing
```

## Deployment

**For CDK deploy (developers):**
```bash
export AWS_PROFILE=your-profile
uv run cdk deploy
```

**To update CloudFormation template:**
```bash
uv run cdk synth
cp cdk.out/VmDatasetPipelineStack.template.json cloudformation/
```

Note: The template contains a hardcoded ECR image URI from account `956728988776`. Cross-account pull permissions are configured on the ECR repo.

## Generator Types

| Prefix | Type | Memory |
|--------|------|--------|
| `O-` | Static/Logic (puzzles, counting) | Low |
| `G-` | Dynamic/Physics (animation, simulation) | High |

G-generators accumulate frames in memory. Adjust batch sizes in `scripts/generator_config.json`.

## Task Message Format

```json
{
  "type": "O-41_nonogram_data-generator",
  "start_index": 0,
  "num_samples": 25,
  "seed": 42,
  "output_format": "tar"
}
```

## Git

- No "Co-Authored-By" or AI-generated descriptions in commits
- Keep commits focused and descriptive
