#script
from mrjob.job import MRJob

class WeatherAnalysis(MRJob):
    def mapper(self, _, line):
        try:
            date, temp, humidity, precipitation = line.split(',')
            temp = float(temp)
            humidity = float(humidity)
            precipitation = float(precipitation)
            yield date, (temp, humidity, precipitation)
        except ValueError:
            pass  # Skip malformed lines

    def reducer(self, date, values):
        total_temp = total_humidity = total_precipitation = 0
        count = 0

        for temp, humidity, precipitation in values:
            total_temp += temp
            total_humidity += humidity
            total_precipitation += precipitation
            count += 1

        avg_temp = total_temp / count
        avg_humidity = total_humidity / count
        avg_precipitation = total_precipitation / count

        if avg_temp > 30:
            condition = "Hot Day"
        elif avg_temp < 10:
            condition = "Cold Day"
        elif avg_precipitation > 5:
            condition = "Rainy Day"
        elif avg_humidity > 80:
            condition = "Humid Day"
        else:
            condition = "Pleasant Day"

        yield date, f"{condition} - Avg Temp: {avg_temp:.1f}°C, Humidity: {avg_humidity:.1f}%, Precipitation: {avg_precipitation:.1f}mm"

if __name__ == "__main__":
    WeatherAnalysis.run()
    
#to run pg3 python weather.py weather_data.csv

