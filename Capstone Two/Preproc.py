import requests
import re
import pandas as pd
import numpy as np
import pickle
from datetime import time, timedelta
from dateutil.parser import parse


class Preprocessing:
    """docstring for ."""

    teams = {'Atlanta Falcons':'atl','Buffalo Bills':'buf',
    'Carolina Panthers':'car','Chicago Bears':'chi','Cincinnati Bengals':'cin',
    'Cleveland Browns':'cle','Indianapolis Colts':'clt',
    'Arizona Cardinals':'crd','Dallas Cowboys':'dal','Denver Broncos':'den',
    'Detroit Lions':'det','Green Bay Packers':'gnb','Houston Texans':'htx',
    'Jacksonville Jaguars':'jax','Kansas City Chiefs':'kan',
    'Miami Dolphins':'mia','Minnesota Vikings':'min','New Orleans Saints':'nor',
    'New England Patriots':'nwe','New York Giants':'nyg','New York Jets':'nyj',
    'Tennessee Titans':'oti','Philadelphia Eagles':'phi',
    'Pittsburgh Steelers':'pit','Oakland Raiders':'rai',
    'Las Vegas Raiders':'rai','St. Louis Rams':'ram','Los Angeles Rams':'ram',
    'Baltimore Ravens':'rav','San Diego Chargers':'sdg',
    'Los Angeles Chargers':'sdg','Seattle Seahawks':'sea',
    'San Francisco 49ers':'sfo','Tampa Bay Buccaneers':'tam',
    'Washington Redskins':'was'}
    org = set(teams.values())

    def __init__(self, start, finish=None):
        self.start = start
        if finish == None:
            finish = start
        self.finish = finish

    def get_codes(self):
        codes = []
        dicdfs = self.dfyear_
        for df in dicdfs.values():
            codes += list(set(df.index.levels[1]))
        self.gcodes_ = codes
        return self.gcodes_

    def get_year(code):
        if (code[4:6] == '01'):
            year = str(int(code[0:4])-1)
        else:
            year = str(int(code[0:4]))
        return year

    def year_codes(codes,yr):
        subset = [codes[i] for i in range(len(codes)) if get_year(codes[i]) == yr]
        return subset

    def get_data_by_year(self):
        scr = Scraper(self.start, self.finish)
        ind = pd.MultiIndex.from_product([self.org,[i for i in range(16)]],
        names=['Team','row'])
        yearcols = ['Code','Home/Away','Opponent','Points','Points_Opp',
        'Yds_Off_Pass','Yds_Off_Rush','Yds_Def_Pass','Yds_Def_Rush','TD',
        'TD_on_Def','FG_Made','FG_Att','RZ_Conv','RZ_Att','RZ_Def_Conv',
        'RZ_Def_Att','Possession','Plays','TO_Gained','TO_Lost','Yds_Pen',
        'Sacks_Def','Tackles_Loss','Yds_per_Kickret','Yds_per_Puntret']
        years = [str(i) for i in range(self.start,self.finish+1)]
        self.gamelog_ = {}
        self.dfyear_ = {}
        for year in years:
            print(f'Scraping {year}...')
            self.gamelog_[year] = scr.get_gamelogs(year)
            print(f'Processing {year}...')
            df=pd.DataFrame(index=ind,columns=yearcols)
            for team in self.org:
                table = self.gamelog_[year][team]
                L0 = list(table.columns.get_level_values(0))
                L0_mod = ['Event' if 'Unnamed' in L0[i] else L0[i] for i in range(len(L0))]
                L0_renmd = dict(zip(L0,L0_mod)) #Rename columns to populate dataframe
                table.rename(columns=L0_renmd,level=0,inplace=True)
# Set up dataframe for team
                dfteam = pd.DataFrame(columns=['Date','Home/Away','Opponent'])
# String dates from "table" are converted to numerical in proper format for use in url
# Special consideration for Janaury, as those dates correspond to following year
                dconv = lambda x: (parse(x + ' ' + str(int(year)+1)).strftime('%Y%m%d')
                                   if 'January' in x else parse(x + ' ' + year).strftime('%Y%m%d'))
                dfteam['Date'] = table['Event']['Date'].apply(dconv)
# Home and Away designation determines how gamecode is labeled
                dfteam['Home/Away'][table.iloc[:,6] == '@'] = 'A'
                dfteam['Home/Away'][table.iloc[:,6] != '@'] = 'H'
                dfteam['Opponent'] = table['Event']['Opp']
# Because of "dfyear" multiindex, cannot simply copy "dfteam" dataframe to "dfyear". Must use .values
                df.loc[team,'Home/Away':'Opponent'] = dfteam[['Home/Away','Opponent']].values
# Gamecodes are calculated to put in list for later url access
# and to put in dfyear dataframe for game identification
                for row in range(16):
                    date = dfteam['Date'][row]
                    if dfteam['Home/Away'][row] == 'H':
                        hometeam = team
                    elif dfteam['Home/Away'][row] == 'A':
                        hometeam = self.teams[dfteam['Opponent'][row]]
                    gamecode = date+'0'+hometeam
                    df.loc[(team,row),'Code'] = gamecode
# Replace row index with gamecodes
            df=df.reset_index(level=1,drop=True)
            df=df.set_index([df.index,df['Code']])
            df.drop('Code',axis=1,inplace=True)
            self.dfyear_[year] = df
        return self.dfyear_

class Scraper(Preprocessing):
    """docstring for ."""

    def __init__(self, start, finish=None):
        super().__init__(start,finish)

    def get_gamelogs(self,year):
        logs = {}
        for team in self.org:
            url = ('https://www.pro-football-reference.com/teams/'+team+'/'
            +year+'/gamelog/')
            res = requests.get(url)
# Scrape table from website
            tableID = 'gamelog'+year
            table=pd.read_html(res.text,attrs={'id':tableID},flavor='bs4')
            logs[team]=table[0]
        return logs


proc = Preprocessing(2010)
df = proc.get_data_by_year()
gamecodes = proc.get_codes()
