"""Unit tests for arborcarbon_utils.common — type coercion helpers and the singleton decorator."""

from __future__ import annotations

import pytest

from arborcarbon_utils.common import singleton, to_bool, to_float, to_int, to_str_upper, truthy


##################################################################################################
# to_bool
##################################################################################################
class TestToBool:
    """Tests for to_bool."""

    @pytest.mark.parametrize("v", [True, False])
    def test_passthrough_bool(self, v):
        """Bool values are returned unchanged."""
        assert to_bool(v) is v

    @pytest.mark.parametrize(
        ("v", "expected"),
        [(1, True), (0, False), (2, True), (-1, True)],
    )
    def test_int_values(self, v, expected):
        """Non-zero int is True; zero is False."""
        assert to_bool(v) is expected

    @pytest.mark.parametrize(
        ("v", "expected"),
        [(1.0, True), (0.0, False), (0.5, True)],
    )
    def test_float_values(self, v, expected):
        """Non-zero float is True; 0.0 is False."""
        assert to_bool(v) is expected

    @pytest.mark.parametrize(
        "v",
        ["1", "true", "True", "TRUE", "t", "T", "yes", "YES", "y", "Y", "on", "ON"],
    )
    def test_truthy_strings(self, v):
        """All recognised truthy tokens (case-insensitive) return True."""
        assert to_bool(v) is True

    @pytest.mark.parametrize(
        "v",
        ["0", "false", "False", "no", "off", "", "maybe", "2"],
    )
    def test_falsy_strings(self, v):
        """Any string not in the truthy token set returns False."""
        assert to_bool(v) is False

    def test_string_with_whitespace(self):
        """Surrounding whitespace is stripped before matching."""
        assert to_bool("  true  ") is True

    def test_unsupported_type_raises(self):
        """Non-bool/int/float/str raises TypeError."""
        with pytest.raises(TypeError):
            to_bool(None)

    def test_list_raises(self):
        """List input raises TypeError."""
        with pytest.raises(TypeError):
            to_bool([True])


##################################################################################################
# to_float
##################################################################################################
class TestToFloat:
    """Tests for to_float."""

    def test_int_to_float(self):
        """int is widened to float."""
        assert to_float(5) == 5.0
        assert isinstance(to_float(5), float)

    def test_float_passthrough(self):
        """float returns the same value."""
        assert to_float(3.14) == 3.14

    def test_string_integer(self):
        """String containing an integer is parsed."""
        assert to_float("42") == 42.0

    def test_string_float(self):
        """String containing a float is parsed."""
        assert to_float("1.5") == 1.5

    def test_string_with_whitespace(self):
        """Surrounding whitespace is stripped."""
        assert to_float("  2.5  ") == 2.5

    def test_negative_value(self):
        """Negative numeric string is parsed correctly."""
        assert to_float("-7.3") == -7.3

    def test_unsupported_type_raises(self):
        """None raises TypeError."""
        with pytest.raises(TypeError):
            to_float(None)

    def test_non_numeric_string_raises(self):
        """Non-numeric string raises ValueError (from float())."""
        with pytest.raises(ValueError):
            to_float("not-a-number")


##################################################################################################
# to_int
##################################################################################################
class TestToInt:
    """Tests for to_int."""

    def test_int_passthrough(self):
        """int is returned as-is."""
        assert to_int(7) == 7

    def test_float_truncated(self):
        """float is truncated toward zero."""
        assert to_int(3.9) == 3
        assert to_int(-3.9) == -3

    def test_string_integer(self):
        """String containing an integer is parsed."""
        assert to_int("100") == 100

    def test_string_with_whitespace(self):
        """Surrounding whitespace is stripped."""
        assert to_int("  50  ") == 50

    def test_negative_string(self):
        """Negative integer string is parsed."""
        assert to_int("-3") == -3

    def test_bool_raises(self):
        """bool is explicitly rejected to avoid silent True→1 / False→0 coercion."""
        with pytest.raises(TypeError):
            to_int(True)
        with pytest.raises(TypeError):
            to_int(False)

    def test_none_raises(self):
        """None raises TypeError."""
        with pytest.raises(TypeError):
            to_int(None)

    def test_float_string_raises(self):
        """String with a decimal point raises ValueError (int() rejects it)."""
        with pytest.raises(ValueError):
            to_int("3.5")


##################################################################################################
# to_str_upper
##################################################################################################
class TestToStrUpper:
    """Tests for to_str_upper."""

    def test_uppercase_string(self):
        """String is uppercased and stripped."""
        assert to_str_upper("hello") == "HELLO"

    def test_mixed_case_with_whitespace(self):
        """Mixed case and surrounding whitespace are normalised."""
        assert to_str_upper("  Hello World  ") == "HELLO WORLD"

    def test_already_upper(self):
        """Already-uppercase string is unchanged."""
        assert to_str_upper("INDEPENDENT") == "INDEPENDENT"

    def test_non_string_coerced(self):
        """Non-string is coerced via str() before uppercasing."""
        assert to_str_upper(123) == "123"
        assert to_str_upper(None) == "NONE"


##################################################################################################
# truthy
##################################################################################################
class TestTruthy:
    """Tests for truthy."""

    @pytest.mark.parametrize("v", ["1", "true", "True", "t", "yes", "y", "on"])
    def test_truthy_strings(self, v):
        """Recognised tokens return True."""
        assert truthy(v) is True

    @pytest.mark.parametrize("v", ["0", "false", "no", "off", "", "maybe"])
    def test_falsy_strings(self, v):
        """Unrecognised tokens and empty string return False."""
        assert truthy(v) is False

    def test_none_returns_false(self):
        """None is treated as absent → False."""
        assert truthy(None) is False

    def test_whitespace_stripped(self):
        """Leading/trailing whitespace is stripped before matching."""
        assert truthy("  true  ") is True
        assert truthy("  false  ") is False


##################################################################################################
# singleton
##################################################################################################
class TestSingleton:
    """Tests for the singleton decorator."""

    def test_same_instance_returned(self):
        """Repeated calls return the identical object."""

        @singleton
        class _Counter:
            def __init__(self):
                self.count = 0

        a = _Counter()
        b = _Counter()
        assert a is b

    def test_constructor_called_once(self):
        """The underlying __init__ runs exactly once regardless of call count."""
        calls = []

        @singleton
        class _Tracker:
            def __init__(self):
                calls.append(1)

        _Tracker()
        _Tracker()
        _Tracker()
        assert len(calls) == 1

    def test_state_shared(self):
        """Mutations made on one reference are visible through another reference."""

        @singleton
        class _Box:
            def __init__(self):
                self.value = 0

        _Box().value = 42
        assert _Box().value == 42
