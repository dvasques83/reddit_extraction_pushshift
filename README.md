
# Create a anaconda environment
conda create --name reddit python=3.9

# Activate the new environ
conda activate reddit

# Install dependencies
pip install -r requirements.txt

# Run script
python extractor.py -name askhistorians -start 2020-03-11 -end 2020-12-31 -subs True -comments True

# For help and usage examples
python extractor.py -h

	Subreddit data extractor v1.0.0

	required arguments:
		-h, --help            show this help message and exit
		-name SUBREDDIT_NAME  Subreddit name. (ex: -name askhistorians)
		-start YYYY-MM-DD     Initial extraction date as string. (ex: -start 2022-03-15)
		-end YYYY-MM-DD       Final extraction date as string. (ex: -end 2022-03-31)

	optional arguments:
		-subs True            Get submissions. (ex: -subs True)
		-comments True        Get comments. (ex: -comments True)