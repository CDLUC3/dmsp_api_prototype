from dmpworks.utils import run_process


def test_run_process_success(caplog):
    cmd = ["echo", "hello world"]
    with caplog.at_level("INFO"):
        run_process(cmd)

    out = caplog.text
    assert "dmpworks.utils:utils.py:30 run_process command: `echo 'hello world'`" in out
    assert "dmpworks.utils:utils.py:40 hello world" in out
