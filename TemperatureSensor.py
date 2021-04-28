import sys
import time


def main(streaming_directory) -> None:
    """ Program that simulates a sensor reading temperatures and writing them into files stored in a directory. The name
    of each file is composed by a time stamp plus the extension '.txt'.

    The temperatures are stored in a list, and they are stored in files in the loop of the program.

    :param streaming_directory:
    :return:
    """
    seconds_to_sleep = 5;
    temperatures = (20.0, 18.7, 18.7, 23.3, 22.1, 20.0, 17.2, 24.0, 12.5, 21.2)
    while True:
        for temperature_value in temperatures:
            # Get current time
            time_stamp = str(time.time())

            # Create a new file name with the time stamp
            file_name = streaming_directory + "/" + time_stamp + ".txt"

            # Write the current temperature value in the file
            with (open(file_name, "w")) as file:
                file.write(str(temperature_value))
            print("Wrote file with temperature " + str(temperature_value))

            # Sleep for a few seconds
            time.sleep(seconds_to_sleep)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python TemperatureSensor directory", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])

