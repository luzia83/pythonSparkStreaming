import sys
import time
import datetime
import random


def main(streaming_directory: str, sensor_identifier: str) -> None:
    """ Program that simulates a sensor reading temperatures and writing them into files stored in a directory. The name
    of each file is composed by a time stamp plus the extension '.csv'. A timestamp is included with the temperature.

    The temperatures are stored in a list, and they are stored in files in the loop of the program.

    :param streaming_directory:
    :param sensor_identifier: name of the sensor
    :return:
    """
    seconds_to_sleep = 10;

    while True:
        # Get current time
        current_time = str(time.time())

        # Create a new file name with the time stamp
        file_name = streaming_directory + "/" + current_time + ".csv"

        # Get the temperature
        temperature_value = random.randrange(15.0, 23.0)

        # Write the current temperature value in the file
        with (open(file_name, "w")) as file:
            time_stamp = datetime.datetime.now();
            file.write(sensor_identifier + "," + str(temperature_value) + "," + str(time_stamp))
        print("Wrote file with temperature " + str(temperature_value))

        # Sleep for a few seconds
        time.sleep(seconds_to_sleep)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python TemperatureSensorWithTimeStamp directory sensorIdentifier", file=sys.stderr)
        exit(-1)

    main(sys.argv[1], sys.argv[2])
