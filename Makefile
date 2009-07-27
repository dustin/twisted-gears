EPYDOC=epydoc-2.6

docs:
	$(EPYDOC) --exclude=test --include-log gearman
