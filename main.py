# project: p5
# submitter: xbu3
# partner: none

import click
from zipfile import ZipFile, ZIP_DEFLATED
from io import TextIOWrapper
import csv
import pandas as pd
import socket, struct
import re
from collections import defaultdict 
import pandas as pd
import geopandas 
import matplotlib.pyplot as plt
import json
from matplotlib.animation import FuncAnimation
from IPython.core.display import HTML
    

def zip_csv_iter(name):
    if name == "IP2LOCATION-LITE-DB1.CSV.ZIP":
        name_csv = "IP2LOCATION-LITE-DB1.CSV"
    else:
        name_csv = name.replace(".zip", ".csv")
    with ZipFile(name) as zf:
        with zf.open(name_csv) as f:
            reader = csv.reader(TextIOWrapper(f))
            for row in reader:
                yield row

def take_ip(list):
    return ip2long(ip_convert(list[0]))

def ip_convert(ip):
    return re.sub(r"[a-zA-Z]", "0", ip)

def ip2long(ip):
    """
    Convert an IP string to long
    code cited from Not_a_Golfer
    """
    packedIP = socket.inet_aton(ip)
    return struct.unpack("!L", packedIP)[0]


@click.command()
@click.argument('zip1')
@click.argument('zip2')
@click.argument('mod', type=click.INT)
def sample(zip1, zip2, mod):
    print("zip1:", zip1)
    print("zip2:", zip2)
    print("mod:", mod)
    reader = zip_csv_iter(zip1)
    count = 0
    header = next(reader)

    with ZipFile(zip2, "w", compression=ZIP_DEFLATED) as zf:
        with zf.open(zip2.replace(".zip", ".csv"), "w") as f:
            data_row = []
            for row in reader:
                if count % mod == 0:
                    data_row.append(row)
                count += 1
            pd.DataFrame(data_row,
                 columns= header).to_csv(TextIOWrapper(f), index=False)

@click.command()
@click.argument('zip1')
@click.argument('zip2')
def country(zip1, zip2):
    print("zip1:", zip1)
    print("zip2:", zip2)
    # sorting
    reader1 = zip_csv_iter(zip1)
    header = next(reader1)
    row1 = list(reader1)
    row1 = sorted(row1, key = take_ip)

    
    #add a new column
    header.append("country")
    reader2 = zip_csv_iter("IP2LOCATION-LITE-DB1.CSV.ZIP")
    row2 = list(reader2)
    index = 0
    for i in row1:
        for j in range(index, len(row2)):
            if take_ip(i) >= int(row2[j][0]) and take_ip(i) <= int(row2[j][1]):
                i.append(row2[j][-1])
                index = j
                break
    
    # write
    with ZipFile(zip2, "w", compression=ZIP_DEFLATED) as zf:
        with zf.open(zip2.replace(".zip", ".csv"), "w") as f:
            pd.DataFrame(row1, columns = header).to_csv(TextIOWrapper(f), index=False)

@click.command()
@click.argument('zipname')
@click.argument('ax')
@click.argument('hour', type=click.INT)
def geohour(zipname, ax=None, hour=None):
    # count occurences per country
    reader = zip_csv_iter(zipname)
    header = next(reader)
    cidx = header.index("country")
    timeidx = header.index("time")
    counts = defaultdict(int)
    w = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    w["count"] = 0
    
    # no Antarctica
    w = w[w["continent"] != "Antarctica"]
    
    # populate counts 
    for row in reader:
        if hour != None:
            if hour != int(row[timeidx].split(":")[0]):
                continue
        counts[row[cidx]] += 1
        
    # handle data
    for country, count in counts.items():
        # sometimes country names in IP dataset don't
        # match names in naturalearth_lowres -- skip those
        if country not in list(w["name"]):
            continue
        
        # add data (either count or a color) to a new column
        w["count"][w["name"] == country] = count   
    
    # json, top 5
    dic = w.set_index("name")["count"].to_dict()
    dic = sorted(dic.items(), key = lambda x: x[1], reverse = True)[:5]
        
    # plot it
    pic = w.plot(column = "count", cmap = "OrRd", figsize = (15, 10), scheme = "quantiles", legend = True)
    pic.set_axis_off()
    fig = pic.get_figure()
    
    # return it
    fig.savefig(ax, format = "svg")
    with open("top_5_h{}.json".format(hour), "w") as f:
        json.dump(dict(dic) ,f)
        
    return fig

@click.command()
@click.argument('zipname')
@click.argument('svg')
@click.argument('continent')
def geocontinent(zipname, svg = None, continent = None):
    # count occurences per country
    reader = zip_csv_iter(zipname)
    header = next(reader)
    cidx = header.index("country")
    counts = defaultdict(int)
    w = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    w["count"] = 0
    
    # no Antarctica
    w = w[w["continent"] != "Antarctica"]
    
    # specific continent countries
    w_continent = w[w["continent"] == continent]
    country_name = list(w_continent["name"])
    
    # populate counts 
    for row in reader:
        if row[cidx] in country_name:
            counts[row[cidx]] += 1
        
    # handle data
    for country, count in counts.items():      
        # add data (either count or a color) to a new column
        w["count"][w["name"] == country] = count   
    
    # json, top 5
    dic = w.set_index("name")["count"].to_dict()
    dic = sorted(dic.items(), key = lambda x: x[1], reverse = True)[:5]
        
    # plot it
    pic = w.plot(facecolor = "blue", edgecolor = "red", column = "count", cmap = "RdYlGn", figsize = (15, 10), scheme = "natural_breaks", legend = True)
    pic.set_axis_off()
    fig = pic.get_figure()
    
    # return it
    fig.savefig(svg, format = "svg")
    with open("top_5_h{}.json".format(continent), "w") as f:
        json.dump(dict(dic) ,f)
        
    return fig

@click.command()
@click.argument('zipname')
@click.argument('html')
def video(zipname, html = None):
    fig, ax = plt.subplots()
    
    def plot_hour(hour):
        # data 
        reader = zip_csv_iter(zipname)
        header = next(reader)
        cidx = header.index("country")
        timeidx = header.index("time")
        counts = defaultdict(int)
        w = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
        # no Antarctica
        w = w[w["continent"] != "Antarctica"]
        w["count"] = 0
        
        # plot the background
        ax.cla()
        w.plot(color = "0.8", ax = ax)

        # populate counts 
        for row in reader:
            if hour != None:
                if hour != int(row[timeidx].split(":")[0]):
                    continue
            counts[row[cidx]] += 1

        # data with count bigger than 0
        for country, count in counts.items():
            if country not in list(w["name"]):
                continue
            w["count"][w["name"] == country] = count   
        
        w = w[w["count"] > 0]
        
        # plot the hour
        w.plot(ax = ax, column = "count", cmap = "Reds", scheme='quantiles')
 
    anim = FuncAnimation(fig, plot_hour, frames = 24, interval = 250)
    html_code = anim.to_html5_video()
    plt.close()
    with open(html, "w") as f:
        f.write(html_code)
    
    
@click.group()
def commands():
    pass


commands.add_command(sample)
commands.add_command(country)
commands.add_command(geohour)
commands.add_command(geocontinent)
commands.add_command(video)

if __name__ == "__main__":
    commands()