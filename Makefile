build:
	rm -rf Dist
	rm -rf ProjectOutput
	rm -rf Data
	mkdir Dist
	cp ProjectCode/main.py ./Dist
	cp ProjectCode/config.yaml ./Dist
	unzip Data.zip -d Dist/
	unzip Data.zip
	cd ProjectCode/src && zip -r ../../Dist/src.zip .
	rm -rf Dist/__MACOSX
	rm -rf __MACOSX
