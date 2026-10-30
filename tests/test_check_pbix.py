import io
import struct
import zipfile

import pytest
from check_pbix import findings, main, read_section_m

PLACEHOLDER_M = """section Section1;
shared ServerHostname = "REPLACE-WITH-SERVER-HOSTNAME" meta [IsParameterQuery = true, Type = "Text"];
shared HttpPath = "REPLACE-WITH-HTTP-PATH" meta [IsParameterQuery = true, Type = "Text"];
shared Catalog = "workspace" meta [IsParameterQuery = true, Type = "Text"];
"""


def make_pbix(path, section_m):
    inner = io.BytesIO()
    with zipfile.ZipFile(inner, "w") as package:
        package.writestr("Formulas/Section1.m", section_m.encode("utf-8-sig"))
    package_bytes = inner.getvalue()
    blob = struct.pack("<II", 0, len(package_bytes)) + package_bytes + struct.pack("<I", 0)
    with zipfile.ZipFile(path, "w") as outer:
        outer.writestr("DataMashup", blob)
        outer.writestr("Version", "1.28")
    return path


def test_placeholders_pass(tmp_path):
    pbix = make_pbix(tmp_path / "ok.pbix", PLACEHOLDER_M)
    assert findings(read_section_m(pbix)) == []
    assert main(["check_pbix.py", str(pbix)]) == 0


def test_real_values_are_flagged_without_printing_them(tmp_path, capsys):
    real = PLACEHOLDER_M.replace("REPLACE-WITH-SERVER-HOSTNAME", "dbc-123.cloud.databricks.com").replace(
        "REPLACE-WITH-HTTP-PATH", "/sql/1.0/warehouses/abc"
    )
    pbix = make_pbix(tmp_path / "bad.pbix", real)
    problems = findings(read_section_m(pbix))
    assert any("ServerHostname" in p for p in problems) and any("HttpPath" in p for p in problems)
    assert any("host or warehouse path" in p for p in problems)
    assert main(["check_pbix.py", str(pbix)]) == 1
    output = capsys.readouterr().out
    assert "dbc-123" not in output and "warehouses/abc" not in output


def make_modern_pbix(path, script_text, encoding):
    with zipfile.ZipFile(path, "w") as outer:
        outer.writestr("DataModel", b"compressed-bytes-not-searched")
        outer.writestr("TMDLScripts/Script%201.tmdl", script_text.encode(encoding))
    return path


def test_modern_format_without_datamashup_is_clean(tmp_path, capsys):
    pbix = make_modern_pbix(
        tmp_path / "new.pbix", 'expression ServerHostname = "REPLACE-WITH-SERVER-HOSTNAME"', "utf-16-le"
    )
    assert read_section_m(pbix) is None
    assert main(["check_pbix.py", str(pbix)]) == 0
    assert "no DataMashup part" in capsys.readouterr().out


@pytest.mark.parametrize("encoding", ["utf-8", "utf-16-le"])
def test_host_in_text_part_is_flagged_without_printing_it(tmp_path, capsys, encoding):
    pbix = make_modern_pbix(tmp_path / "new.pbix", 'ServerHostname = "dbc-123.cloud.databricks.com"', encoding)
    assert main(["check_pbix.py", str(pbix)]) == 1
    output = capsys.readouterr().out
    assert "TMDLScripts/Script%201.tmdl" in output and "dbc-123" not in output


def test_missing_parameters_are_flagged(tmp_path):
    pbix = make_pbix(tmp_path / "missing.pbix", 'section Section1;\nshared Catalog = "workspace" meta [];\n')
    problems = findings(read_section_m(pbix))
    assert any("ServerHostname not found" in p for p in problems)


def test_unreadable_file_fails(tmp_path, capsys):
    broken = tmp_path / "broken.pbix"
    broken.write_bytes(b"not a zip")
    assert main(["check_pbix.py", str(broken)]) == 1
    assert main(["check_pbix.py", str(tmp_path / "absent.pbix")]) == 1


def test_wrong_arguments_print_usage():
    assert main(["check_pbix.py"]) == 1


@pytest.mark.parametrize("version", [1, 2])
def test_unsupported_datamashup_version_is_rejected(tmp_path, version):
    path = tmp_path / "v.pbix"
    with zipfile.ZipFile(path, "w") as outer:
        outer.writestr("DataMashup", struct.pack("<II", version, 0))
    with pytest.raises(ValueError):
        read_section_m(path)
