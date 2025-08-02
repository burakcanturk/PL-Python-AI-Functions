CREATE EXTENSION IF NOT EXISTS plpython3u;

CREATE INDEX IF NOT EXISTS id_idnex ON Comments (id ASC);

DROP FUNCTION IF EXISTS DetectLanguages(table_name VARCHAR(255), comment_column VARCHAR(255), translate_language VARCHAR(255));
--TABLE(comment TEXT, language VARCHAR(255))

CREATE OR REPLACE FUNCTION DetectLanguages(table_name VARCHAR(255), comment_column VARCHAR(255), translate_language VARCHAR(255) DEFAULT 'tr')
RETURNS TABLE(id INT, comment TEXT, language VARCHAR(255), translated TEXT) /*TEXT*/
AS $$ 

	import os
	from time import sleep
	import asyncio
	from googletrans import Translator

	with_socket = True

	plan = plpy.prepare(f"SELECT id, {comment_column} FROM {table_name} ORDER BY id ASC LIMIT 10")

	result = plpy.execute(plan)

	comments = []
	ids = []

	for i in result:
		comments.append(i[comment_column])
		ids.append(i["id"])

	ret_val = []

	#translator = Translator()

	async def detect_language(text):
		translator = Translator()
		result = await translator.translate(text, dest = translate_language)
		return result.src, result.text

	i = 0
	
	try_range = 10

	while i < len(comments):
		try:
			try_range = 10
			if comments[i] != "":
				lang, translated = asyncio.run(detect_language(comments[i]))
				ret_val.append((ids[i], comments[i], lang, translated))
				plpy.notice(f"i = {i}")
			else:
				ret_val.append((ids[i], comments[i], "empty", ""))
				plpy.notice(f"Empty {i}")
		except:
			if try_range == 0:
				ret_val.append((ids[i], comments[i], "error", ""))
				plpy.notice(f"The error level has reached max level! Breaking ...")
				break
			plpy.notice(f"An error has detected! Trying again {try_range}...")
			try_range -= 1
			continue
		i += 1

	plpy.notice(f"Done!!!")

	return ret_val

$$ LANGUAGE plpython3u;

DROP FUNCTION IF EXISTS
CommentAnalyze(table_name VARCHAR(255), comment_column VARCHAR(255), words VARCHAR(255)[]);

CREATE OR REPLACE FUNCTION CommentAnalyze(table_name VARCHAR(255), comment_column VARCHAR(255), words VARCHAR(255)[])
RETURNS TABLE(id INT, comment TEXT, analyzed_values JSONB) /*TEXT*/ /*TABLE(analyzed_values JSONB[])*/
AS $$ 

	import os
	import json
	from transformers import pipeline
	import torch

	device = 0 if torch.cuda.is_available() else -1  # 0: GPU, -1: CPU

	with_socket = True

	classifier = pipeline(
                "zero-shot-classification",
                model = "joeddav/xlm-roberta-large-xnli",
                tockenizer = "joeddav/xlm-roberta-large-xnli",
                device = device)

	plan = plpy.prepare(f"SELECT id, {comment_column} FROM {table_name} /*WHERE CHAR_LENGTH({comment_column}) < 25*/ ORDER BY id ASC /*LIMIT 10*/")

	result = plpy.execute(plan)

	comments = []
	ids = []

	for i in result:
		comments.append(i[comment_column])
		ids.append(i["id"])

	values = []

	ret_val = []

	i = 0

	try_range = 10

	values = []

	while i < len(comments):

		try:

			if comments[i] != "":

				response = classifier(comments[i], 
									candidate_labels=words,
									multi_label = True)
				values = {}
				for x in range(len(response['labels'])):
					print(response['labels'][x], response['scores'][x])
					values[response['labels'][x]] = response['scores'][x]
				values = json.dumps(values)
				ret_val.append((ids[i], comments[i], values))
				plpy.notice(f"Successful {i}")
	
			else:
				ret_val.append((ids[i], comments[i], json.dumps({})))
				plpy.notice(f"Empty {i}")
	
			i += 1
	
			try_range = 10

		except:
			if try_range == 0:
				ret_val.append((ids[i], comments[i], [{"error": ""}]))
				plpy.notice(f"The error level has reached max level! Breaking...")
				break
			try_range -= 1
			plpy.notice(f"An error has detected! Trying again {try_range}...")
			continue

	plpy.notice(f"Done!!!")
	
	return ret_val

$$ LANGUAGE plpython3u;

DROP TABLE IF EXISTS translated_comments;

CREATE TABLE IF NOT EXISTS translated_comments AS
SELECT * FROM DetectLanguages("Comments", "comment", "en");

DROP TABLE IF EXISTS translated_titles;

CREATE TABLE IF NOT EXISTS translated_titles AS
SELECT * FROM DetectLanguages("Comments", "title", "en");

DROP TABLE IF EXISTS analyzed_comments;

CREATE TABLE IF NOT EXISTS analyzed_comments AS
SELECT * FROM CommentAnalyze('translated_comments', 'translated', ARRAY['money', 'car', 'weather', 'other']);

DROP TABLE IF EXISTS analyzed_titles;

CREATE TABLE IF NOT EXISTS analyzed_titles AS
SELECT * FROM CommentAnalyze('translated_titles', 'translated', ARRAY['money', 'car', 'weather', 'other']);

SELECT * FROM analyzed_titles;

SELECT * FROM translated_comments;
SELECT * FROM translated_title;
SELECT * FROM analyzed_comments;

SELECT * FROM analyzed_title;
