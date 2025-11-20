# Entry point for running the FreshKart pipeline with spark-submit.

from pipeline.orchestrator import run


if __name__ == "__main__":
    run()
