from pathlib import Path

from notebooks.pipeline import file_management


def test_build_input_file_path_uses_prefix_and_extension(tmp_path, monkeypatch):
    monkeypatch.setattr(file_management, "INPUT_DIR", tmp_path)
    path = file_management.build_input_file_path("2024-01-01")
    assert path == tmp_path / "orders_2024-01-01.json"


def test_move_to_done_moves_file(tmp_path, monkeypatch):
    monkeypatch.setattr(file_management, "INPUT_DIR", tmp_path)
    done_dir = tmp_path / "done"
    monkeypatch.setattr(file_management, "DONE_DIR", done_dir)
    source = file_management.build_input_file_path("2024-01-02")
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("data")

    dest = file_management.move_to_done("2024-01-02")

    assert dest == done_dir / source.name
    assert not source.exists()
    assert dest.exists()


def test_move_to_error_handles_arbitrary_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(file_management, "ERROR_DIR", tmp_path)
    src = tmp_path / "bad.txt"
    src.write_text("bad")

    moved = file_management.move_path_to_error(src)

    assert moved == tmp_path / "bad.txt"
    assert moved.exists()
    assert not src.exists()
