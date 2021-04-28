import sys
import time
import os
import requests


if len(sys.argv) != 4:
    print("Usage: python FetchFileByInterval "
          "<URL to fetch> <streaming directory> <number of seconds>", file=sys.stderr)
    exit(-1)
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

urlToFetch = sys.argv[1]
streamingDirectory = sys.argv[2]
secondsToSleep = int(sys.argv[3])
while True:
    time_stamp = str(time.time())

    newFile = streamingDirectory + "/" + str(time_stamp).replace(".","") + ".csv"
    response = requests.get(urlToFetch, headers=headers)
    data = response.content.decode("utf-8")
    with open(os.path.join(streamingDirectory, newFile), "w") as file1:
        file1.write(data)
    time.sleep(secondsToSleep)
