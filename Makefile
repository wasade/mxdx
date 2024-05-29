# ----------------------------------------------------------------------------
# Copyright (c) 2014--, mxdx development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

TEST_COMMAND = pytest

.PHONY: lint test

test:
	$(TEST_COMMAND)
	./usage-test.sh

lint:
	ruff check
