'''
Fotmob Extractor Script
----------------------------------------------
Main Author: Kelly's Football Bets

This script contains the functions that populate the 
data we use in our research.
'''

###################################################
# Required Libraries

from asyncio.log import logger
from winreg import ConnectRegistry
from numpy import mat
import pandas as pd
import requests
import json
from datetime import datetime
from datetime import timedelta
import time
from multiprocessing import Pool
from bs4 import BeautifulSoup
from soupsieve import match
import tqdm
import logging
from random import randint

###################################################
# These functions get the match ids from Fotmob.

###################################################

def fotmob_match_ids_on_date(date = datetime.today().strftime("%Y%m%d")):
    '''
    This function gets all the Fotmob match id's for a certain date.

    Args:
     - date (str): the date in "%Y%m%d" format.
    '''

    # Request the data.
    url = f"https://www.fotmob.com/api/matches?date={date}"

    headers = {
        "authority": "www.fotmob.com",
        "accept": "application/json, text/plain, */*",
        "sec-ch-ua-mobile": "?0",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
        "sec-fetch-site": "same-origin",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty"
    }

    response = requests.request("GET", url, headers=headers)

    data = json.loads(response.content)
    match_data = []

    for league in data["leagues"]:
        league_id = league["primaryId"]
        for match in league["matches"]:
            match_dict = {k:v for k,v in match.items() if k in ["id", "time", "tournamentStage"]}
            match_dict.update({
                "leagueId": league_id,
                "H": match["home"]["id"], 
                "A": match["away"]["id"],
                "finished": match["status"]["finished"],
                "cancelled": match["status"]["cancelled"]
            })
            match_data.append(match_dict)

    match_data = pd.DataFrame(match_data)
    match_data["date"] = pd.to_datetime(match_data["time"], format = "%d.%m.%Y %H:%M")
    match_data.drop(["time"], axis = 1, inplace = True)

    # Format table to tidy format
    match_data = match_data.melt(id_vars=["id", "leagueId", "tournamentStage", "finished", "cancelled", "date"])
    match_data.rename(columns={"id":"match_id","variable":"venue","value":"team_id"}, inplace = True)

    return match_data


def fotmob_match_ids_historical_generator(from_date = datetime(2018,8,1)):
    '''
    This function generates a dataframe with all the match id's from a
    certain date.

    Args:
     - from_date (datetime object): the date since when we want to scrape
    '''

    date_list = pd.date_range(start = from_date, end = datetime.today())
    date_list = [x.strftime("%Y%m%d") for x in date_list]
    
    pool = Pool(processes=8)
    matches = pd.concat(tqdm.tqdm(pool.imap_unordered(fotmob_match_ids_on_date, date_list), total = len(date_list)))
    
    pool.close()
    pool.join()

    return matches


def fotmob_match_ids_catalog_update():
    '''
    This function updates the match_id catalog with finished matches.
    '''

    try:
        df = pd.read_csv("data/database/match_ids.csv", dtype = {"team_id":int, "match_id":int, "leagueId":int})
        df["date"] = pd.to_datetime(df["date"])
        max_date = df["date"].max()
    except Exception:
        print("Couldn't read CSV file. Starting from scratch, initial date: 2018-07-01.")
        df = pd.DataFrame()
        max_date = datetime(2018,6,30)

    n_initial = df.shape[0]
    matches = fotmob_match_ids_historical_generator(from_date = max_date - timedelta(days = 3))
    matches = pd.concat([df, matches], sort = False)
    matches = matches[(matches["finished"]) & (~matches["cancelled"])].copy()
    matches.drop_duplicates(inplace = True)
    matches.reset_index(drop = True, inplace = True)

    matches.to_csv("data/database/match_ids.csv", index = False)
    n = matches.shape[0]
    print(f"{n - n_initial} new matches recorded in our database.")


def fotmob_match_overview_on_date(date = datetime.today().strftime("%Y%m%d"), league_filters = [47,54,87,55,53,108]):
    '''
    This function prints a dataframe of all the Fotmob matches for a certain date.
    League filters can be applied with a list.

    Args:
     - date (str): the date in "%Y%m%d" format.
     - league_filters (list): a list of league id's to filter.
    '''

    # Request the data.
    url = f"https://www.fotmob.com/api/matches?date={date}"

    headers = {
        "authority": "www.fotmob.com",
        "accept": "application/json, text/plain, */*",
        "sec-ch-ua-mobile": "?0",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
        "sec-fetch-site": "same-origin",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty"
    }

    response = requests.request("GET", url, headers=headers)

    data = json.loads(response.content)
    match_data = []

    for league in data["leagues"]:
        league_id = league["primaryId"]
        for match in league["matches"]:
            match_dict = {k:v for k,v in match.items() if k in ["id", "leagueId", "time", "tournamentStage"]}
            match_dict.update({
                "leagueId": league_id,
                "home_team_id": match["home"]["id"], 
                "away_team_id": match["away"]["id"],
                "home_team_name": match["home"]["name"], 
                "away_team_name": match["away"]["name"],
            })
            match_data.append(match_dict)

    match_data = pd.DataFrame(match_data)
    match_data["date"] = pd.to_datetime(match_data["time"], format = "%d.%m.%Y %H:%M")
    match_data.drop(["time"], axis = 1, inplace = True)

    match_data = match_data[match_data["leagueId"].isin(league_filters)].reset_index(drop = True)
    match_data.rename(columns = {"id":"match_id"}, inplace=True)

    return match_data


###################################################
# These functions scrape match data from Fotmob

###################################################

def fotmob_match_stats(match_id):
    '''
    This function gets the main statistics for a certain match given
    its match_id

    Args:
     - match_id (int): the Fotmob match id.
    '''

    url = "https://www.fotmob.com/matchDetails"

    querystring = {"ccode3":"MEX","timezone":"America/Mexico_City","matchId":str(match_id)}

    headers = {
        "authority": "www.fotmob.com",
        "accept": "application/json, text/plain, */*",
        "sec-ch-ua-mobile": "?0",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
        "sec-fetch-site": "same-origin",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty"
    }


    try:
        response = requests.request("GET", url, headers=headers, params=querystring)
        data = json.loads(response.content)
    except Exception as e:
        print(e)
        time.sleep(randint(1,10))
        try:
            response = requests.request("GET", url, headers=headers, params=querystring)
            data = json.loads(response.content)
        except Exception as e:
            print(e)
            print(f"{match_id} failed. Please keep this in mind.")
            return pd.DataFrame()

    general_stats = data["header"]["teams"]

    home_team = general_stats[0]["name"]
    home_team_id = general_stats[0]["id"]
    home_team_score = general_stats[0]["score"]
    away_team = general_stats[1]["name"]
    away_team_id = general_stats[1]["id"]
    away_team_score = general_stats[1]["score"]

    general_stats = data["content"]["matchFacts"]["infoBox"]

    match_date = f"{general_stats['Match Date']['dateFormatted'].split(', ')[1]} {general_stats['Match Date']['timeFormatted']}"
    match_date = match_date.replace("a.m.","AM")
    match_date = match_date.replace("p.m.","PM")
    match_date = match_date.replace("noon","PM")
    match_date = datetime.strptime(match_date, "%b %d %Y %I:%M %p")

    # Referee is not captured for all leagues.

    try:
        referee = general_stats["Referee"]["text"]
    except Exception:
        referee = None
    
    # We also capture Line-ups

    try:
        home_team_lineup = data["content"]["lineup"]["lineup"][0]["lineup"]
        away_team_lineup = data["content"]["lineup"]["lineup"][1]["lineup"]
    except Exception:
        home_team_lineup = None
        away_team_lineup = None

    try:

        match_data = {
            "match_id": match_id,
            "home_team_id" : home_team_id,
            "away_team_id" : away_team_id,
            "home_team_name" : home_team,
            "away_team_name" : away_team,
            "home_team_score" : home_team_score,
            "away_team_score" : away_team_score,
            "home_team_xG" : None,
            "away_team_xG" : None,
            "home_team_xGOT" : None,
            "away_team_xGOT" : None,
            "home_team_passesOwn" : None,
            "away_team_passesOwn" : None,
            "home_team_passesOpp" : None,
            "away_team_passesOpp" : None,
            "home_team_corners" : None,
            "away_team_corners" : None,
            "home_team_fouls" : None,
            "away_team_fouls" : None,
            "home_team_shots" : None,
            "away_team_shots" : None,
            "home_team_shotsOT" : None,
            "away_team_shotsOT" : None,
            "home_team_yellowC" : None,
            "away_team_yellowC" : None,
            "home_team_redC" : None,
            "away_team_redC" : None,
            "home_team_lineup": home_team_lineup,
            "away_team_lineup": away_team_lineup,
            "date": match_date,
            "referee": referee
        }

        
        all_stats = data["content"]["stats"]["stats"]
        
        for index in all_stats:
            if "TOP" in index["title"]:
                for stat in index["stats"]:
                    if "xG" in stat["title"]:
                        match_data["home_team_xG"] = float(stat["stats"][0])
                        match_data["away_team_xG"] = float(stat["stats"][1])
                    elif "shots" in stat["title"]:
                        match_data["home_team_shots"] = stat["stats"][0]
                        match_data["away_team_shots"] = stat["stats"][1]
                    elif "ouls" in stat["title"]:
                        match_data["home_team_fouls"] = stat["stats"][0]
                        match_data["away_team_fouls"] = stat["stats"][1]
                    elif "orners" in stat["title"]:
                        match_data["home_team_corners"] = stat["stats"][0]
                        match_data["away_team_corners"] = stat["stats"][1]
                    else:
                        continue
            elif "EXPECTED GOALS" in index["title"]:
                for stat in index["stats"]:
                    if "xGOT" in stat["title"]:
                        match_data["home_team_xGOT"] = stat["stats"][0]
                        match_data["away_team_xGOT"] = stat["stats"][1]
                    else:
                        continue
            elif "PASSES" in index["title"]:
                for stat in index["stats"]:
                    if "Own" in stat["title"]:
                        match_data["home_team_passesOwn"] = stat["stats"][0]
                        match_data["away_team_passesOwn"] = stat["stats"][1]
                    elif "Opposition" in stat["title"]:
                        match_data["home_team_passesOpp"] = stat["stats"][0]
                        match_data["away_team_passesOpp"] = stat["stats"][1]                     
                    else:
                        continue             
            elif "SHOTS" in index["title"]:
                for stat in index["stats"]:
                    if "on target" in stat["title"]:
                        match_data["home_team_shotsOT"] = stat["stats"][0]
                        match_data["away_team_shotsOT"] = stat["stats"][1]
                    else:
                        continue
            elif "DISCIPLINE" in index["title"]:
                for stat in index["stats"]:
                    if "ellow" in stat["title"]:
                        match_data["home_team_yellowC"] = stat["stats"][0]
                        match_data["away_team_yellowC"] = stat["stats"][1]
                    elif "ed" in stat["title"]:
                        match_data["home_team_redC"] = stat["stats"][0]
                        match_data["away_team_redC"] = stat["stats"][1]
                    else:
                        continue
    except:
        return {}
    

    return match_data


def fotmob_match_stats_pool(match_ids, melted = True):

    if not isinstance(match_ids, list):
        match_ids = [match_ids]
    
    match_ids = [str(x) for x in match_ids]
    
    pool = Pool(processes=8)
    matches = list(tqdm.tqdm(pool.imap_unordered(fotmob_match_stats, match_ids), total = len(match_ids)))
    pool.close()
    pool.join()

    matches = pd.DataFrame(matches)
    matches.dropna(inplace = True, how = "all")
    matches.reset_index(drop = True, inplace = True)

    if melted:
        matches = matches.melt(id_vars = ["match_id","date","referee"]).copy()

        # We keep the core variables
        core_vars = matches[~matches["variable"].isin(["home_team_id","home_team_name","home_team_lineup","away_team_id","away_team_name","away_team_lineup"])].copy()
        core_vars.loc[:,"venue"] = ["H" if "home" in x else "A" for x in core_vars["variable"]]
        core_vars.loc[:,"variable"] = [x[2] + "_for" for x in core_vars["variable"].str.split("_")]

        # We do the opposite for the against stats
        core_vars_ag = core_vars.copy()
        core_vars_ag.loc[:,"venue"] = ["A" if x == "H" else "H" for x in core_vars_ag["venue"]]
        core_vars_ag.loc[:,"variable"] = [x.replace("for", "ag") for x in core_vars_ag["variable"]]

        core_vars = pd.concat([core_vars, core_vars_ag])

        matches_aux = core_vars.copy()

        # Add the team_id as a reference
        for x in ["team_id", "team_name", "team_lineup"]:
            home_team = matches[matches["variable"].str.contains(f"home_{x}")][["value","match_id"]]
            away_team = matches[matches["variable"].str.contains(f"away_{x}")][["value","match_id"]]
            home_team.rename(columns = {"value":f"{x}"}, inplace = True)
            away_team.rename(columns = {"value":f"{x}"}, inplace = True)
            home_team.loc[:,"venue"] = "H"
            away_team.loc[:,"venue"] = "A"
            teams = pd.concat([home_team, away_team])
            matches_aux = pd.merge(matches_aux, teams, how = "right", on = ["venue", "match_id"])
        
        matches = matches_aux.copy()
      
    return (matches)
