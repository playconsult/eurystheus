all : clean dist

dist :
	python setup.py sdist

clean :
	rm -rf dist