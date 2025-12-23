import pandas as pd
import matplotlib.pyplot as plt
import statistics
import numpy as np
from collections import Counter
import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)
from multiprocessing import Pool, cpu_count
import time
import json
import networkx as nx
import requests


class HonkaiStatistics_APOC:
    def __init__(self, version, floor, node=0, by_ed=6,by_ed_inclusive=False,by_ed_inclusive_combined=False, by_score =0, by_char = None, by_scores_combined = 0,not_char=False,sustain_condition=None,star_num=None):
        self.version = version
        url = f"https://huggingface.co/datasets/LvlUrArti/MocData/resolve/main/{version}_as.csv"
        self.df = pd.read_csv(url)

        url = "https://raw.githubusercontent.com/LvlUrArti/MocStats/main/data/characters.json"
        response = requests.get(url)
        response.raise_for_status()  # raise error if request fails
        info = response.json()  # parse JSON     
        self.rol = pd.DataFrame.from_dict(info, orient='index')
        
               
        # Initialize dictionary
        self.teams = {}
        self.chars = {}
        self.archetypes={}
        self.individual_teams = {}
        self.individual_archetypes= {}
        self.combined_teams = {}
        self.combined_chars = {}
        self.combined_archetypes ={}
        self.cyc = {}
        self.cyc_combined = {}
        self.not_char = not_char
        self.total_samples = 0
        self.floor = floor
        self.node = node
        self.by_ed = by_ed
        self.by_ed_inclusive = by_ed_inclusive
        self.by_ed_inclusive_combined = by_ed_inclusive_combined
        self.by_score = by_score
        self.by_scores_combined = by_scores_combined
        self.by_char = by_char
        self.sustain_condition = sustain_condition
        self.star_num = star_num
        
        if self.star_num:
            self.df = self.df[(self.df['star_num'] == self.star_num)]
        
        # Filter dataframe for specific floor and node
        if self.node == 0:
            self.df = self.df[(self.df['floor'] == self.floor)]
        else:
            self.df = self.df[(self.df['floor'] == self.floor) & (self.df['node'] == self.node)]
        self._process_data()

    def _process_data(self):
        exempts = ["Kafka", "Jing Yuan", "Seele", "Jingliu", "Dan Heng • Imbibitor Lunae",
                   "Blade", "Argenti", "Topaz & Numby", "Ruan Mei", "Silver Wolf", 
                   "Fu Xuan", "Luocha", "Huohuo", "Dr. Ratio", "Black Swan", 
                   "Sparkle", "Acheron", "Aventurine", "Robin", "Boothill", 
                   "Firefly", "Yunli", "Jiaoqiu", "Feixiao", "Lingsha","Rappa","Sunday","Fugue",
                   "The Herta","Aglaea","Tribbie","Mydei","Castorice","Anaxa","Hyacine","Cipher","Phainon","Archer","Saber","Hysilens","Cerydra","Evernight","Dan Heng • Permansor Terrae","Cyrene"]
        
        # Loop through characters and round_num
        index = 0
        sustains= [ "Fu Xuan", "Luocha", "Huohuo", "Aventurine", "Lingsha", "Gallagher","Bailu", "Gepard", "Lynx","Natasha","Hyacine","Dan Heng • Permansor Terrae"]
        for (p,x, y, z, w, i, cons1, cons2, cons3, cons4) in (zip(self.df['uid'],self.df['ch1'], self.df['ch2'], 
                                                              self.df['ch3'], self.df['ch4'], 
                                                              self.df['round_num'], 
                                                              self.df['cons1'], self.df['cons2'], 
                                                              self.df['cons3'], self.df['cons4'])):
            if not self.by_ed_inclusive:
                if (x in exempts and cons1 > self.by_ed) or \
                (y in exempts and cons2 > self.by_ed) or \
                (z in exempts and cons3 > self.by_ed) or \
                (w in exempts and cons4 > self.by_ed):
                    continue
                
            else:
                n=[x, y, z, w]    
                ei = [cons1,cons2,cons3,cons4]
                maxei =0 
                for (v,e) in zip(n,ei):
                    if v not in exempts:
                        continue
                    
                    if e > maxei:
                        maxei = e
                        
                if maxei != self.by_ed:
                    continue
            if i < self.by_score:
                continue
            
            if self.by_char:
                if self.not_char:
                    if self.by_char in [x,y,z,w]:
                        continue
                elif self.by_char not in [x, y, z, w]:
                    continue 
                
            n=[x, y, z, w]    
            ei = [cons1,cons2,cons3,cons4]
            # Append individual character
            add =0
            if self.sustain_condition ==True:
                
                if not bool(set(sustains) & set(n)):
                    continue
                add=1    
            elif self.sustain_condition ==False:
                
                if bool(set(sustains) & set(n)):
                    continue
            else:
                if bool(set(sustains) & set(n)):
                    add = 1  
                    
            maxei =0 
            for (v,e) in zip(n,ei):
                if v not in exempts:
                    continue
                
                if e > maxei:
                    maxei = e
                    
                
                
            if i not in self.cyc:
                self.cyc[i] = {'Eidolons':{0:0,1:0 ,2:0,3:0,4:0,5:0,6:0}}
                
            self.cyc[i]['Eidolons'][maxei]+=1   
            
            
            for (v,e ) in zip(n,ei):
                if v not in self.chars:
                    # Initialize dictionary entry for the character
                    self.chars[v] = {'Samples': 0, 'Scores': [], 'uids': [],'Eidolons':{0:0,1:0 ,2:0,3:0,4:0,5:0,6:0}, "Index":index,"Sustains":0}
                    index+=1
                
                import math
                if not math.isnan(e):
                    # Update Eidolons
                    self.chars[v]['Eidolons'][e] +=1
                
                # Update the sample count
                
                self.chars[v]['Samples'] += 1

                # Append the round_num (i) to the 'Scores' list
                self.chars[v]['Scores'].append(i)

                # Append uids to the uids list
                self.chars[v]['uids'].append(p)
                
                self.chars[v]['Sustains']+=add
                   
            # Sort and handle missing or NaN values
            n = sorted([(x), y, z, w], key=lambda d: self.chars[d]['Index'])
            n = tuple(n)  # Convert to tuple to use as a key in the dictionary
            
            arch = [x for x in n if x is not None
                and x in self.rol.index
                and self.rol.loc[x, 'role'] == "Damage Dealer"]
            
            arch_t = tuple(arch)
                
            # Initialize nested dictionary for the team if not already present
            if n not in self.teams:
                self.teams[n] = {'Samples': 0, 'Scores': [], 'uids':[]}
            
            # Update the sample count
            self.teams[n]['Samples'] += 1
            
            # Append the round_num (i) to the 'Scores' list
            self.teams[n]['Scores'].append(i)
            
            # Appends uids to the uids list
            self.teams[n]['uids'].append(p)
            
            

            # Initialize nested dictionary for the team if not already present
            if arch_t not in self.archetypes:
                self.archetypes[arch_t] = {'Samples': 0, 'Scores': [], 'uids':[]}
            
            # Update the sample count
            self.archetypes[arch_t]['Samples'] += 1
            
            # Append the round_num (i) to the 'Scores' list
            self.archetypes[arch_t]['Scores'].append(i)
            
            # Appends uids to the uids list
            self.archetypes[arch_t]['uids'].append(p)
            
            if self.node ==0:
                if p not in self.individual_teams:
                    self.individual_teams[p] = {'Teams':[],'Scores': 0,"Max Eidolon":maxei}
                    self.individual_archetypes[p] = {'Archetypes':[],'Scores': 0,"Max Eidolon":maxei}
                    
                self.individual_teams[p]['Teams'].append(n)
                self.individual_teams[p]['Scores'] += i
                self.individual_archetypes[p]['Archetypes'].append(arch_t)
                self.individual_archetypes[p]['Scores'] += i
                
                if maxei > self.individual_teams[p]["Max Eidolon"]:
                    self.individual_teams[p]['Max Eidolon'] = maxei
                    self.individual_archetypes[p]['Max Eidolon'] = maxei

                
                
                
        if self.node ==0:   
            dc = self.individual_teams
            bc = self.individual_archetypes
            for x,y in zip(dc,bc):
                if (self.by_ed_inclusive_combined == False and len(dc[x]['Teams']) ==2) or\
                    (self.by_ed_inclusive_combined == True and (dc[x]['Max Eidolon']) ==self.by_ed and len(dc[x]['Teams']) ==2):
            
                        g = tuple(dc[x]['Teams'])
                        g1 = tuple(bc[y]['Archetypes'])
                        
                        if self.individual_teams[x]['Scores'] < self.by_scores_combined:
                            continue
            
                        if g not in self.combined_teams:
                            self.combined_teams[g] = {'Samples': 0, 'Scores': [], 'uids':[]}
                        
                        if g1 not in self.combined_archetypes:
                            self.combined_archetypes[g1] = {'Samples': 0, 'Scores': [], 'uids':[]}
                                
                        # Update the sample count
                        self.combined_teams[g]['Samples'] += 1
                        self.combined_archetypes[g1]['Samples'] += 1
                        
                        # Append the round_num (i) to the 'Avg Cycles' list
                        self.combined_teams[g]['Scores'].append(dc[x]['Scores'])
                        self.combined_archetypes[g1]['Scores'].append(dc[y]['Scores'])
                        
                        
                        # Appends uids to the uids list
                        self.combined_teams[g]['uids'].append(x)
                        self.combined_archetypes[g1]['uids'].append(y)
                        
                        if dc[x]['Scores'] not in self.cyc_combined:
                            self.cyc_combined[dc[x]['Scores']] = {'Eidolons':{0:0,1:0 ,2:0,3:0,4:0,5:0,6:0}}
                    
                        self.cyc_combined[dc[x]['Scores']]['Eidolons'][dc[x]['Max Eidolon']]+=1  
               

                    

        from itertools import chain,product
        
        for pair in self.combined_teams:
            pairs = chain(product(pair[0],pair[1]))
            for j in pairs:
                if j not in self.combined_chars:
                    self.combined_chars[j] = {'Samples': 0, 'Scores': [], 'uids':[]}
                
                # Update the sample count
                self.combined_chars[j]['Samples'] += self.combined_teams[pair]['Samples'] 
                
                # Append the round_num (i) to the 'Scores' list
                self.combined_chars[j]['Scores'].extend(self.combined_teams[pair]['Scores'])
                
                # Appends uids to the uids list
                self.combined_chars[j]['uids'].extend(self.combined_teams[pair]['uids'])
                        
        flatten = ([(v['uids']) for v in self.teams.values()])
        flatten2 = ([(v['uids']) for v in self.combined_chars.values()])
        self.total_samples = len(set(list(chain(*flatten))))
        self.total_samples2 = len(set(list(chain(*flatten2))))
      
        
    def print_appearance_rates(self,by_avg_score = False,by_max_score = False,by_char1 = None,by_char2 = None,by_char3 = None,by_char4 = None,least=None,
                               no_sustains=False,output=True , not_by_char1 = False , not_by_char2 = False, not_by_char3 = False, not_by_char4 = False):
        
        sustains= [ "Fu Xuan", "Luocha", "Huohuo", "Aventurine", "Lingsha", "Gallagher","Bailu", "Gepard", "Lynx","Natasha","Hyacine","Dan Heng • Permansor Terrae"]
        m = []
        p = []
        if by_char1:
            if not not_by_char1:
                m.append(by_char1)
            else:
                p.append(by_char1)
        if by_char2:
            if not not_by_char2:
                m.append(by_char2)
            else:
                p.append(by_char2)
        if by_char3:
            if not not_by_char3:
                m.append(by_char3)
            else:
                p.append(by_char3)
        if by_char4:
            if not not_by_char4:
                m.append(by_char4)
            else:
                p.append(by_char4)
            
        # Sorting the dictionary by the 'Samples' value in descending order
        if by_avg_score:
            sorted_teams = dict(sorted(self.teams.items(), key=lambda i: np.median(i[1]['Scores']), reverse=True))
        
        elif by_max_score:
            sorted_teams = dict(sorted(self.teams.items(), key=lambda i: max(i[1]['Scores']), reverse=True))
        else:
            sorted_teams = dict(sorted(self.teams.items(), key=lambda i: i[1]['Samples'], reverse=True))
            
        # Initialize a list to store rows of data
        rows = []
        
        # Print appearance rate and sample size for each team
        if output:
            print(f"Total Samples {self.total_samples} by team for Apocalyptic Shadow version {self.version} , Node {self.node} , over/equal {self.by_score} score up to {self.by_ed} Eidolon, floor {self.floor}")
        for rank, (team, data) in enumerate(sorted_teams.items(), start=1):
            # Apply filtering if character filters are provided
            

            # Calculate appearance rate
            appearance_rate = (data['Samples'] / self.total_samples) * 100

            # Format the team list with single quotes around each character
            formatted_team = [f"'{char}'" for char in team]
        
            # Add a row to the list
            rows.append({
            'Rank': rank,
            'Team': f"({', '.join(formatted_team)})",
            'Appearance Rate (%)': round(appearance_rate, 2),
            'Samples': data['Samples'],
            'Min Score': min(data['Scores']),
            '25th Percentile': round(np.percentile(data['Scores'], 25), 2),
            'Median Score': round(np.median(data['Scores']), 2),
            '75th Percentile': round(np.percentile(data['Scores'], 75), 2),
            'Average Score': round(np.average(data['Scores']), 2),
            'Std Dev': round(np.std(data['Scores'], ddof=1), 2),  # Uses sample standard deviation
            'Max Score': max(data['Scores']),
        })


        # Create a DataFrame from the list of rows
        df = pd.DataFrame(rows)
        if no_sustains:
            df = df[df['Team'].apply(lambda x: not any(item in x for item in sustains))]
   
        if least:
            df =df[(df['Samples'] >=least)]
            df['Rank'] = range(1,len(df['Rank'])+1)
        if m:
            df = df[df['Team'].apply(lambda x: all(item in x for item in m))]
        if p:
            df = df[df['Team'].apply(lambda x: not any(item in x for item in p))]    
            
        # Printing the DataFrame
        if output:
            print(df.to_string(index=False))
            return
        return df 
    
    def print_archetypes(self,by_avg_score = False,by_max_score = False,by_char1 = None,by_char2 = None,by_char3 = None,by_char4 = None,least=None,
                               output=True , not_by_char1 = False , not_by_char2 = False, not_by_char3 = False, not_by_char4 = False):
        
        m = []
        p = []
        if by_char1:
            if not not_by_char1:
                m.append(by_char1)
            else:
                p.append(by_char1)
        if by_char2:
            if not not_by_char2:
                m.append(by_char2)
            else:
                p.append(by_char2)
        if by_char3:
            if not not_by_char3:
                m.append(by_char3)
            else:
                p.append(by_char3)
        if by_char4:
            if not not_by_char4:
                m.append(by_char4)
            else:
                p.append(by_char4)
            
        # Sorting the dictionary by the 'Samples' value in descending order
        if by_avg_score:
            sorted_archetypes = dict(sorted(self.archetypes.items(), key=lambda i: np.median(i[1]['Scores']), reverse=True))
        
        elif by_max_score:
            sorted_archetypes = dict(sorted(self.archetypes.items(), key=lambda i: max(i[1]['Scores']), reverse=True))
        else:
            sorted_archetypes = dict(sorted(self.archetypes.items(), key=lambda i: i[1]['Samples'], reverse=True))
            
        # Initialize a list to store rows of data
        rows = []
        
        # Print appearance rate and sample size for each team
        if output:
            print(f"Total Samples {self.total_samples} by Archetype for Apocalyptic Shadow version {self.version} , Node {self.node} , over/equal {self.by_score} score up to {self.by_ed} Eidolon, floor {self.floor}")
        for rank, (archetype, data) in enumerate(sorted_archetypes.items(), start=1):
            # Apply filtering if character filters are provided
            

            # Calculate appearance rate
            appearance_rate = (data['Samples'] / self.total_samples) * 100

            # Format the team list with single quotes around each character
            formatted_archetype = [f"'{char}'" for char in archetype]
        
            # Add a row to the list
            rows.append({
            'Rank': rank,
            'Archetype': f"({', '.join(formatted_archetype)})",
            'Appearance Rate (%)': round(appearance_rate, 2),
            'Samples': data['Samples'],
            'Min Score': min(data['Scores']),
            '25th Percentile': round(np.percentile(data['Scores'], 25), 2),
            'Median Score': round(np.median(data['Scores']), 2),
            '75th Percentile': round(np.percentile(data['Scores'], 75), 2),
            'Average Score': round(np.average(data['Scores']), 2),
            'Std Dev': round(np.std(data['Scores'], ddof=1), 2),  # Uses sample standard deviation
            'Max Score': max(data['Scores']),
        })


        # Create a DataFrame from the list of rows
        df = pd.DataFrame(rows)
        
        if least:
            df =df[(df['Samples'] >=least)]
            df['Rank'] = range(1,len(df['Rank'])+1)
        if m:
            df = df[df['Archetype'].apply(lambda x: all(item in x for item in m))]
        if p:
            df = df[df['Archetype'].apply(lambda x: not any(item in x for item in p))]    
            
        # Printing the DataFrame
        if output:
            print(df.to_string(index=False))
            return
        return df 
    
    def print_archetypes_both_sides(self,by_avg_points = False,by_max_points = False, 
                                          by_char1_n1 = None,by_char2_n1 = None,by_char3_n1 = None,by_char4_n1 = None,
                                          by_char1_n2 = None,by_char2_n2 = None,by_char3_n2 = None,by_char4_n2 = None,
                                          least=None,output=True):
        if self.node != 0:
            return "Node needs to be set to 0"
        
        m = []
        g= []
        if by_char1_n1:
            m.append(by_char1_n1)
        if by_char2_n1:
            m.append(by_char2_n1)
        if by_char3_n1:
            m.append(by_char3_n1)
        if by_char4_n1:
            m.append(by_char4_n1)
            
        if by_char1_n2:
            g.append(by_char1_n2)
        if by_char2_n2:
            g.append(by_char2_n2)
        if by_char3_n2:
            g.append(by_char3_n2)
        if by_char4_n2:
            g.append(by_char4_n2)
            
        # Sorting the dictionary by the 'Samples' value in descending order
        if by_avg_points:
            sorted_archetypes = dict(sorted(self.combined_archetypes.items(), key=lambda i: np.mean(i[1]['Scores']), reverse=True))
        
        elif by_max_points:
            sorted_archetypes = dict(sorted(self.combined_archetypes.items(), key=lambda i: max(i[1]['Scores']), reverse=True))
        else:
            sorted_archetypes = dict(sorted(self.combined_archetypes.items(), key=lambda i: i[1]['Samples'], reverse=True))
        
        # Initialize a list to store rows of data
        rows = []
        
        # Print appearance rate and sample size for each team
        if output:
            print(f"Total Samples {self.total_samples2} by archetype for Apocalyptic Shadow version {self.version}, over/equal {self.by_score} score up to {self.by_ed} Eidolon, floor {self.floor}")
        for rank, (archetype, data) in enumerate(sorted_archetypes.items(), start=1):
            # Apply filtering if character filters are provided

            # Calculate appearance rate
            appearance_rate = (data['Samples'] / self.total_samples2) * 100

            # Add a row to the list
            rows.append({
            'Rank': rank,
            'Archetype 1': f"{archetype[0]}",
            'Archetype 2': f"{archetype[1]}",
            'Appearance Rate (%)': round(appearance_rate, 2),
            'Samples': data['Samples'],
            'Min Score': min(data['Scores']),
            '25th Percentile': round(np.percentile(data['Scores'], 25), 2),
            'Median Score': round(np.median(data['Scores']), 2),
            '75th Percentile': round(np.percentile(data['Scores'], 75), 2),
            'Average Score': round(np.average(data['Scores']), 2),
            'Std Dev': round(np.std(data['Scores'], ddof=1), 2),  # Uses sample standard deviation
            'Max Score': max(data['Scores']),
        })


          
        # Create a DataFrame from the list of rows
        df = pd.DataFrame(rows)

        # Filter by minimum sample size
        if least:
            df = df[df['Samples'] >= least]
            df['Rank'] = range(1, len(df) + 1)

        
        # Filter by character inclusion (search as substrings in the strings)
        if m:
            df = df[df['Archetype 1'].apply(lambda x: all(char in x for char in m)) ]
            
        if g:
            df = df[df['Archetype 2'].apply(lambda x: all(char in x for char in g)) ]

        # Printing the DataFrame
        if output:
            print(df.to_string(index=False))
            return
        return df
    
    def print_appearance_rates_both_sides(self,by_avg_points = False,by_max_points = False, 
                                          by_char1_n1 = None,by_char2_n1 = None,by_char3_n1 = None,by_char4_n1 = None,
                                          by_char1_n2 = None,by_char2_n2 = None,by_char3_n2 = None,by_char4_n2 = None,
                                          least=None,no_sustains=False,output=True):
        if self.node != 0:
            return "Node needs to be set to 0"
        sustains= [ "Fu Xuan", "Luocha", "Huohuo", "Aventurine", "Lingsha", "Gallagher","Bailu", "Gepard", "Lynx","Natasha"]
        m = []
        g= []
        if by_char1_n1:
            m.append(by_char1_n1)
        if by_char2_n1:
            m.append(by_char2_n1)
        if by_char3_n1:
            m.append(by_char3_n1)
        if by_char4_n1:
            m.append(by_char4_n1)
            
        if by_char1_n2:
            g.append(by_char1_n2)
        if by_char2_n2:
            g.append(by_char2_n2)
        if by_char3_n2:
            g.append(by_char3_n2)
        if by_char4_n2:
            g.append(by_char4_n2)
            
        # Sorting the dictionary by the 'Samples' value in descending order
        if by_avg_points:
            sorted_teams = dict(sorted(self.combined_teams.items(), key=lambda i: np.mean(i[1]['Scores']), reverse=True))
        
        elif by_max_points:
            sorted_teams = dict(sorted(self.combined_teams.items(), key=lambda i: max(i[1]['Scores']), reverse=True))
        else:
            sorted_teams = dict(sorted(self.combined_teams.items(), key=lambda i: i[1]['Samples'], reverse=True))
        
        # Initialize a list to store rows of data
        rows = []
        
        # Print appearance rate and sample size for each team
        if output:
            print(f"Total Samples {self.total_samples2} by team for Apocalyptic Shadow version {self.version}, over/equal {self.by_score} score up to {self.by_ed} Eidolon, floor {self.floor}")
        for rank, (team, data) in enumerate(sorted_teams.items(), start=1):
            # Apply filtering if character filters are provided

            # Calculate appearance rate
            appearance_rate = (data['Samples'] / self.total_samples2) * 100

            # Format the team list with single quotes around each character
            formatted_team = [f"'{char}'" for char in team]
        
            # Add a row to the list
            rows.append({
            'Rank': rank,
            'Team 1': f"{team[0]}",
            'Team 2': f"{team[1]}",
            'Appearance Rate (%)': round(appearance_rate, 2),
            'Samples': data['Samples'],
            'Min Score': min(data['Scores']),
            '25th Percentile': round(np.percentile(data['Scores'], 25), 2),
            'Median Score': round(np.median(data['Scores']), 2),
            '75th Percentile': round(np.percentile(data['Scores'], 75), 2),
            'Average Score': round(np.average(data['Scores']), 2),
            'Std Dev': round(np.std(data['Scores'], ddof=1), 2),  # Uses sample standard deviation
            'Max Score': max(data['Scores']),
        })


          
        # Create a DataFrame from the list of rows
        df = pd.DataFrame(rows)

        # Filter out rows with any sustain in either team (when columns are strings)
        if no_sustains:
            df = df[~(df['Team 1'].apply(lambda x: any(s in x for s in sustains)) |
                    df['Team 2'].apply(lambda x: any(s in x for s in sustains)))]

        # Filter by minimum sample size
        if least:
            df = df[df['Samples'] >= least]
            df['Rank'] = range(1, len(df) + 1)

        
        # Filter by character inclusion (search as substrings in the strings)
        if m:
            df = df[df['Team 1'].apply(lambda x: all(char in x for char in m)) ]
            
        if g:
            df = df[df['Team 2'].apply(lambda x: all(char in x for char in g)) ]

        # Printing the DataFrame
        if output:
            print(df.to_string(index=False))
            return
        return df
            
    def print_appearance_rate_by_char(self,by_avg_score = False,by_max_score = False,output = True,damage_dealers_only=False):
        # Sorting the dictionary by the 'Samples' value in descending order
        if by_avg_score:
            sorted_chars = dict(sorted(self.chars.items(), key=lambda i: np.median(i[1]['Scores']), reverse=True))
        
        elif by_max_score:
            sorted_chars = dict(sorted(self.chars.items(), key=lambda i: max(i[1]['Scores']), reverse=True))
        else:
            sorted_chars = dict(sorted(self.chars.items(), key=lambda i: i[1]['Samples'],reverse = True))
            
        # Printing the header
        if output:
            print(f"Total Samples {self.total_samples} of characters for Apocalyptic Shadow version {self.version}, Node {self.node}, under/equal {self.by_score} scores up to {self.by_ed} Eidolon, floor {self.floor}")

        # Creating a list to store data for the DataFrame
        data = []

        for char, char_data in sorted_chars.items():
            appearance_rate = (char_data['Samples'] / self.total_samples) * 100
            sustain_rate = (char_data['Sustains']/char_data['Samples'] ) * 100
            # Calculate Eidolon rates (appearance rate for each Eidolon level)
            eidolon_rates = {f'Eidolon {e} (%)': round((count / char_data['Samples']) * 100, 2) 
                            for e, count in char_data['Eidolons'].items()}

            # Append the data for the character including Eidolon columns
            data.append({
                    'Rank': len(data) + 1,
                    'Character': char,
                    'Appearance Rate (%)': round(appearance_rate, 3),
                    'Samples': char_data['Samples'],
                    'Min Score': min(char_data['Scores']),
                    '25th Percentile': round(np.percentile(char_data['Scores'], 25), 2),
                    'Median Score': round(np.median(char_data['Scores']), 2),
                    '75th Percentile': round(np.percentile(char_data['Scores'], 75), 2),
                    'Average Score': round(np.average(char_data['Scores']), 2),
                    'Std Dev': round(np.std(char_data['Scores'], ddof=1), 2),  # Uses sample standard deviation
                    'Max Score': max(char_data['Scores']),
                    'Sustain Samples': char_data['Sustains'],
                    "Sustain_Percentage":round(sustain_rate, 2),
                    **eidolon_rates  # Unpacks the Eidolon columns
                })

        # Creating the DataFrame
        df = pd.DataFrame(data)
        pd.set_option('display.width', 1000)  # Adjust the total width of the output
        pd.set_option('display.max_columns', None)  # Ensure all columns are shown
        
        
       
        rol = self.rol
        if damage_dealers_only:
             df = df[df['Character'].apply(lambda x: pd.notna(x) and x in rol.index and rol.loc[x, 'role'] == "Damage Dealer")]
             df['Rank'] = range(1, len(df) + 1)
        if output:
            print(df.to_string(index=False))
            return
        return df
    def print_appearance_rate_by_char_both_sides(self,by_avg_score = False,by_max_score = False,output = True,damage_dealers_only=True,ch1_filter=None,ch2_filter=None):
        # Sorting the dictionary by the 'Samples' value in descending order
        if by_avg_score:
            sorted_chars = dict(sorted(self.combined_chars.items(), key=lambda i: np.median(i[1]['Scores']), reverse=True))
        
        elif by_max_score:
            sorted_chars = dict(sorted(self.combined_chars.items(), key=lambda i: max(i[1]['Scores']), reverse=True))
        else:
            sorted_chars = dict(sorted(self.combined_chars.items(), key=lambda i: i[1]['Samples'],reverse = True))
            
        # Printing the header
        if output:
            print(f"Total Samples {self.total_samples2} of characters for Apocalyptic Shadow version {self.version}, Node {self.node}, under/equal {self.by_score} scores up to {self.by_ed} Eidolon, floor {self.floor}")

        # Creating a list to store data for the DataFrame
        data = []

        for char_pair, char_data in sorted_chars.items():
            appearance_rate = (char_data['Samples'] / self.total_samples2) * 100

            # Unpack characters from the tuple
            if isinstance(char_pair, tuple) and len(char_pair) == 2:
                char1, char2 = char_pair
            else:
                char1, char2 = char_pair, None

            data.append({
                    'Rank': len(data) + 1,
                    'Character 1': char1,
                    'Character 2': char2,
                    'Appearance Rate (%)': round(appearance_rate, 2),
                    'Samples': char_data['Samples'],
                    'Min Score': min(char_data['Scores']),
                    '25th Percentile': round(np.percentile(char_data['Scores'], 25), 2),
                    'Median Score': round(np.median(char_data['Scores']), 2),
                    '75th Percentile': round(np.percentile(char_data['Scores'], 75), 2),
                    'Average Score': round(np.average(char_data['Scores']), 2),
                    'Std Dev': round(np.std(char_data['Scores'], ddof=1), 2),  # Uses sample standard deviation
                    'Max Score': max(char_data['Scores']),
                  
                })

        # Creating the DataFrame
        df = pd.DataFrame(data)
        pd.set_option('display.width', 1000)  # Adjust the total width of the output
        pd.set_option('display.max_columns', None)  # Ensure all columns are shown
        
        
       
        rol = self.rol
        if damage_dealers_only:
             df = df[df['Character 1'].apply(lambda x: pd.notna(x) and x in rol.index and rol.loc[x, 'role'] == "Damage Dealer")]
             df = df[df['Character 2'].apply(lambda x: pd.notna(x) and x in rol.index and rol.loc[x, 'role'] == "Damage Dealer")]
             df['Rank'] = range(1, len(df) + 1)
        if ch1_filter:
            df = df[df['Character 1'] == ch1_filter]
        if ch2_filter:
            df = df[df['Character 2'] == ch2_filter]     
        if output:
            print(df.to_string(index=False))
            return
        return df
                    
    def print_uids_by_team(self, team):
        # Print the list of UIDs by team
       
        new = self.df
        fil = new[(new['uid'].isin(self.teams[team]['uids'])) & (new['floor'] == self.floor)]
        pd.set_option('display.width', 1000)  # Adjust the total width of the output
        pd.set_option('display.max_columns', None)  # Ensure all columns are shown

        # Printing the DataFrame
        print(fil.to_string(index=False))
        
    def print_uids_by_scores(self, score):
        # Print the list of UIDs by team
        hon2 = HonkaiStatistics_APOC(version = self.version, floor = self.floor,node = self.node,by_ed=self.by_ed,by_ed_inclusive=self.by_ed_inclusive,by_score=score,by_char=self.by_char,not_char=self.not_char)
        print(f"Score:{score}\nEidolon:{self.by_ed} \nNode:{self.node} \nFloor:{self.floor}")

        
        for team in hon2.teams:
             print(f"\nTeam:{team}")
             hon2.print_uids_by_team(team)
             
    def print_uids_by_team_combined(self, team):
        # Print the list of UIDs by team

        new = self.df
        fil = new[(new['uid'].isin(self.combined_teams[team]['uids'])) & (new['floor'] == self.floor)]
        pd.set_option('display.width', 1000)  # Adjust the total width of the output
        pd.set_option('display.max_columns', None)  # Ensure all columns are shown

        # Printing the DataFrame
        print(fil.to_string(index=False))
        
    def print_uids_by_scores_combined(self, score):
        # Print the list of UIDs by team
        hon2 = HonkaiStatistics_APOC(version = self.version, floor = self.floor,node = self.node,by_ed=self.by_ed,by_ed_inclusive=self.by_ed_inclusive,by_scores_combined=score,by_char=self.by_char)
        print(f"Score:{score}\nEidolon:{self.by_ed} \nNode:{self.node} \nFloor:{self.floor}")

        
        for team in hon2.combined_teams:
             print(f"\nTeams:{team}")
             hon2.print_uids_by_team_combined(team)
             
    def print_uids_by_archetypes_combined(self, archetype):
        # Print the list of UIDs by team
      
        new = self.df
        fil = new[(new['uid'].isin(self.combined_archetypes[archetype]['uids'])) & (new['floor'] == self.floor)]
        pd.set_option('display.width', 1000)  # Adjust the total width of the output
        pd.set_option('display.max_columns', None)  # Ensure all columns are shown

        # Printing the DataFrame
        print(fil.to_string(index=False))
        
    def print_uids_by_scores_combined(self, score):
        # Print the list of UIDs by team
        hon2 = HonkaiStatistics_APOC(version = self.version, floor = self.floor,node = self.node,by_ed=self.by_ed,by_ed_inclusive=self.by_ed_inclusive,by_scores_combined=score,by_char=self.by_char)
        print(f"Score:{score}\nEidolon:{self.by_ed} \nNode:{self.node} \nFloor:{self.floor}")

        
        for team in hon2.combined_teams:
             print(f"\nTeams:{team}")
             hon2.print_uids_by_team_combined(team)
                             
    def print_uids_by_archetype(self, archetype):
        # Print the list of UIDs by team
        self.csv_path = fr".\MocStats\data\raw_csvs\{self.version}_as.csv"
        self.df = pd.read_csv(self.csv_path)
        new = self.df
        fil = new[(new['uid'].isin(self.archetypes[archetype]['uids'])) & (new['floor'] == self.floor)]
        pd.set_option('display.width', 1000)  # Adjust the total width of the output
        pd.set_option('display.max_columns', None)  # Ensure all columns are shown

        # Printing the DataFrame
        print(fil.to_string(index=False))
        
              
    def plot_statistics(self, team_key):
        team_key = tuple(team_key)  # Convert list to tuple for dictionary lookup
        if team_key in self.teams:
            scores = self.teams[team_key]['Scores']
            sample_size = self.teams[team_key]['Samples']  # Get the sample size
            if scores:
                # Calculate statistics
                median = statistics.median(scores)
                mode = statistics.mode(scores) if len(scores) > 1 else scores[0]
                mean = statistics.mean(scores)
                std_dev = statistics.stdev(scores) if len(scores) > 1 else 0
                
                # Print the Counter of all scores sorted by score value
                score_counts = Counter(scores)
                print(f"Score Counts for Team {team_key}:")
                total = 0
                print(f"Sample Size: {sample_size}")
                
                for score_value in sorted(score_counts,reverse=True):  # Sort by score value
                    count = score_counts[score_value]
                    total += count
                    # Calculate the percentile based on the total counts
                    percentile = (1 - (total / sample_size)) * 100
                    print(f"Scores: {score_value}, Count: {count}, Percentile: {percentile:.2f}%")
                    
                # Create frequency graph (histogram)
                plt.figure(figsize=(12, 6))
                plt.hist(scores, bins='auto', alpha=0.5, color='blue', edgecolor='black', label='Scores Frequency')
                
                # Overlay statistics on the histogram
                plt.axvline(mean, color='orange', linestyle='dashed', linewidth=1, label=f'Mean: {mean:.2f}')
                plt.axvline(median, color='green', linestyle='dashed', linewidth=1, label=f'Median: {median:.2f}')
                plt.axvline(mode, color='red', linestyle='dashed', linewidth=1, label=f'Mode: {mode:.2f}')
                plt.axvline(mean + std_dev, color='purple', linestyle='dashed', linewidth=1, label=f'Std Dev: {std_dev:.2f}')
                plt.axvline(mean - std_dev, color='purple', linestyle='dashed', linewidth=1)

                # Add titles and labels for histogram
                plt.title(f"Score Frequency for Team {team_key} for version {self.version}, Node {self.node} , up to {self.by_ed} Eidolon")
                plt.xlabel('Score')
                plt.ylabel('Frequency')
                plt.legend()

                # Display sample size on the histogram
                plt.text(mean, max(plt.ylim()) * 0.8, f'Sample Size: {sample_size}', 
                         horizontalalignment='center', fontsize=10, color='black')

                plt.show()
            else:
                print(f"No score data available for team {team_key}")
        else:
            print(f"Team {team_key} not found in the dictionary")
            
    def plot_statistics_char(self, char):
        
        if char in self.chars:
            scores = self.chars[char]['Scores']
            sample_size = self.chars[char]['Samples']  # Get the sample size
            if scores:
                # Calculate statistics
                median = statistics.median(scores)
                mode = statistics.mode(scores) if len(scores) > 1 else scores[0]
                mean = statistics.mean(scores)
                std_dev = statistics.stdev(scores) if len(scores) > 1 else 0
                
                # Print the Counter of all scores sorted by score value
                score_counts = Counter(scores)
                print(f"Score Counts for Character {char}:")
                total = 0
                print(f"Sample Size: {sample_size}")
                
                for score_value in sorted(score_counts,reverse=True):  # Sort by score value
                    count = score_counts[score_value]
                    total += count
                    # Calculate the percentile based on the total counts
                    percentile = (1 - (total / sample_size)) * 100
                    print(f"Scores: {score_value}, Count: {count}, Percentile: {percentile:.2f}%")
                    
                # Create frequency graph (histogram)
                plt.figure(figsize=(12, 6))
                plt.hist(scores, bins='auto', alpha=0.5, color='blue', edgecolor='black', label='Scores Frequency')
                
                # Overlay statistics on the histogram
                plt.axvline(mean, color='orange', linestyle='dashed', linewidth=1, label=f'Mean: {mean:.2f}')
                plt.axvline(median, color='green', linestyle='dashed', linewidth=1, label=f'Median: {median:.2f}')
                plt.axvline(mode, color='red', linestyle='dashed', linewidth=1, label=f'Mode: {mode:.2f}')
                plt.axvline(mean + std_dev, color='purple', linestyle='dashed', linewidth=1, label=f'Std Dev: {std_dev:.2f}')
                plt.axvline(mean - std_dev, color='purple', linestyle='dashed', linewidth=1)

                # Add titles and labels for histogram
                plt.title(f"Scores Frequency for Character {char} for version {self.version}, Node {self.node} , up to {self.by_ed} Eidolon")
                plt.xlabel('Scores')
                plt.ylabel('Frequency')
                plt.legend()

                # Display sample size on the histogram
                plt.text(mean, max(plt.ylim()) * 0.8, f'Sample Size: {sample_size}', 
                         horizontalalignment='center', fontsize=10, color='black')

                plt.show()
            else:
                print(f"No score data available for char {char}")
        else:
            print(f"Character {char} not found in the dictionary") 
            
    def plot_statistics_archetype(self, archetype):
        
        if archetype in self.archetypes:
            scores = self.archetypes[archetype]['Scores']
            sample_size = self.archetypes[archetype]['Samples']  # Get the sample size
            if scores:
                # Calculate statistics
                median = statistics.median(scores)
                mode = statistics.mode(scores) if len(scores) > 1 else scores[0]
                mean = statistics.mean(scores)
                std_dev = statistics.stdev(scores) if len(scores) > 1 else 0
                
                # Print the Counter of all scores sorted by score value
                score_counts = Counter(scores)
                print(f"Score Counts for Archetype {archetype}:")
                total = 0
                print(f"Sample Size: {sample_size}")
                
                for score_value in sorted(score_counts,reverse=True):  # Sort by score value
                    count = score_counts[score_value]
                    total += count
                    # Calculate the percentile based on the total counts
                    percentile = (1 - (total / sample_size)) * 100
                    print(f"Scores: {score_value}, Count: {count}, Percentile: {percentile:.2f}%")
                    
                # Create frequency graph (histogram)
                plt.figure(figsize=(12, 6))
                plt.hist(scores, bins='auto', alpha=0.5, color='blue', edgecolor='black', label='Scores Frequency')
                
                # Overlay statistics on the histogram
                plt.axvline(mean, color='orange', linestyle='dashed', linewidth=1, label=f'Mean: {mean:.2f}')
                plt.axvline(median, color='green', linestyle='dashed', linewidth=1, label=f'Median: {median:.2f}')
                plt.axvline(mode, color='red', linestyle='dashed', linewidth=1, label=f'Mode: {mode:.2f}')
                plt.axvline(mean + std_dev, color='purple', linestyle='dashed', linewidth=1, label=f'Std Dev: {std_dev:.2f}')
                plt.axvline(mean - std_dev, color='purple', linestyle='dashed', linewidth=1)

                # Add titles and labels for histogram
                plt.title(f"Scores Frequency for Archetype {archetype} for version {self.version}, Node {self.node} , up to {self.by_ed} Eidolon")
                plt.xlabel('Scores')
                plt.ylabel('Frequency')
                plt.legend()

                # Display sample size on the histogram
                plt.text(mean, max(plt.ylim()) * 0.8, f'Sample Size: {sample_size}', 
                         horizontalalignment='center', fontsize=10, color='black')

                plt.show()
            else:
                print(f"No score data available for Archetype {archetype}")
        else:
            print(f"Archetype {archetype} not found in the dictionary") 
            
    def plot_statistics_all(self,culmitive=False):
        cycles = []
        for char in self.teams:
            cycles += self.teams[char]['Scores']

        if cycles:
            # Calculate statistics
            median = statistics.median(cycles)
            mode = statistics.mode(cycles) if len(cycles) > 1 else cycles[0]
            mean = statistics.mean(cycles)
            std_dev = statistics.stdev(cycles) if len(cycles) > 1 else 0
            
            # Counter of all cycles
            cycle_counts = Counter(cycles)
            total = 0
            sample_size = len(cycles)

            
            # Collect rows for DataFrame
            rows = []
            cul = {x:0 for x in range(7) }
            for cycle_value in sorted(cycle_counts,reverse=True):
                count = cycle_counts[cycle_value]
                total += count
                percentile = (1 - (total / sample_size)) * 100
                eidolons = self.cyc[cycle_value]['Eidolons']

                # Create one row with separate columns for each eidolon level
                row = {
                    "Cycles": cycle_value,
                    "Count": count,
                    "Percentile (%)": round(percentile, 2)
                }

                # Add percentage columns E0–E6
                for i in range(7):
                    e_count = eidolons.get(i, 0)
                    
                    if culmitive:
                        row[f"E{i} (%)"] = round(((e_count+cul[i]) / total) * 100, 2) if count > 0 else 0
                        cul[i]+=e_count
                    else:
                        row[f"E{i} (%)"] = round((e_count / count) * 100, 2) if count > 0 else 0

                rows.append(row)

            # Convert to DataFrame
            df = pd.DataFrame(rows)

            # Optional: reorder columns for readability
            df = df[["Cycles", "Count", "Percentile (%)",
                    "E0 (%)", "E1 (%)", "E2 (%)", "E3 (%)", "E4 (%)", "E5 (%)", "E6 (%)"]]

            # Print DataFrame
            print(f"Sample Size: {sample_size}")
            print(df.to_string(index=False))

            # Create histogram
            plt.figure(figsize=(12, 6))
            plt.hist(cycles, bins='auto', alpha=0.5, color='blue', edgecolor='black', label='Scores Frequency')
            
            plt.axvline(mean, color='orange', linestyle='dashed', linewidth=1, label=f'Mean: {mean:.2f}')
            plt.axvline(median, color='green', linestyle='dashed', linewidth=1, label=f'Median: {median:.2f}')
            plt.axvline(mode, color='red', linestyle='dashed', linewidth=1, label=f'Mode: {mode:.2f}')
            plt.axvline(mean + std_dev, color='purple', linestyle='dashed', linewidth=1, label=f'Std Dev: {std_dev:.2f}')
            plt.axvline(mean - std_dev, color='purple', linestyle='dashed', linewidth=1)

            plt.title(f"Avg Cycles Frequency for all for version {self.version}, Node {self.node}, up to {self.by_ed} Eidolon")
            plt.xlabel('Avg Cycles')
            plt.ylabel('Frequency')
            plt.legend()
            plt.text(mean, max(plt.ylim()) * 0.8, f'Sample Size: {sample_size}',
                        horizontalalignment='center', fontsize=10, color='black')
            plt.show()
        else:
            print("No cycle data available")
        
            
    def plot_statistics_all_combined(self,culmitive=False):
        cycles = []
        for char in self.combined_teams:
            cycles += self.combined_teams[char]['Scores']

        if cycles:
            # Calculate statistics
            median = statistics.median(cycles)
            mode = statistics.mode(cycles) if len(cycles) > 1 else cycles[0]
            mean = statistics.mean(cycles)
            std_dev = statistics.stdev(cycles) if len(cycles) > 1 else 0
            
            # Counter of all cycles
            cycle_counts = Counter(cycles)
            total = 0
            sample_size = len(cycles)

            
            # Collect rows for DataFrame
            rows = []
            cul = {x:0 for x in range(7) }
            for cycle_value in sorted(cycle_counts,reverse=True):
                count = cycle_counts[cycle_value]
                total += count
                percentile = (1 - (total / sample_size)) * 100
                eidolons = self.cyc_combined[cycle_value]['Eidolons']

                # Create one row with separate columns for each eidolon level
                row = {
                    "Cycles": cycle_value,
                    "Count": count,
                    "Percentile (%)": round(percentile, 2)
                }

                # Add percentage columns E0–E6
                for i in range(7):
                    e_count = eidolons.get(i, 0)
                    
                    if culmitive:
                        row[f"E{i} (%)"] = round(((e_count+cul[i]) / total) * 100, 2) if count > 0 else 0
                        cul[i]+=e_count
                    else:
                        row[f"E{i} (%)"] = round((e_count / count) * 100, 2) if count > 0 else 0

                rows.append(row)

            # Convert to DataFrame
            df = pd.DataFrame(rows)

            # Optional: reorder columns for readability
            df = df[["Cycles", "Count", "Percentile (%)",
                    "E0 (%)", "E1 (%)", "E2 (%)", "E3 (%)", "E4 (%)", "E5 (%)", "E6 (%)"]]

            # Print DataFrame
            print(f"Sample Size: {sample_size}")
            print(df.to_string(index=False))

            # Create histogram
            plt.figure(figsize=(12, 6))
            plt.hist(cycles, bins='auto', alpha=0.5, color='blue', edgecolor='black', label='Scores Frequency')
            
            plt.axvline(mean, color='orange', linestyle='dashed', linewidth=1, label=f'Mean: {mean:.2f}')
            plt.axvline(median, color='green', linestyle='dashed', linewidth=1, label=f'Median: {median:.2f}')
            plt.axvline(mode, color='red', linestyle='dashed', linewidth=1, label=f'Mode: {mode:.2f}')
            plt.axvline(mean + std_dev, color='purple', linestyle='dashed', linewidth=1, label=f'Std Dev: {std_dev:.2f}')
            plt.axvline(mean - std_dev, color='purple', linestyle='dashed', linewidth=1)

            plt.title(f"Avg Cycles Frequency for all for version {self.version}, Node {self.node}, up to {self.by_ed} Eidolon")
            plt.xlabel('Avg Cycles')
            plt.ylabel('Frequency')
            plt.legend()
            plt.text(mean, max(plt.ylim()) * 0.8, f'Sample Size: {sample_size}',
                        horizontalalignment='center', fontsize=10, color='black')
            plt.show()
        else:
            print("No cycle data available")
            
            
    def plot_percentile_intersection(self,scoresA, scoresB, 
                                 labelA='Dataset A', labelB='Dataset B'):
        """
        Plots percentile-score curves for two datasets (different lengths allowed)
        and marks the intersection point where A overtakes B.
        
        Parameters:
        - scoresA: array-like, values for dataset A
        - scoresB: array-like, values for dataset B
        - labelA: label for dataset A
        - labelB: label for dataset B
        
        Returns:
        - p_star: intersection percentile (float, 0-100)
        - score_star: score at intersection (float)
        """
        scoresA = np.sort(np.array(scoresA, dtype=float).flatten())
        scoresB = np.sort(np.array(scoresB, dtype=float).flatten())

        # Generate common percentiles (0-100)
        percentiles = np.linspace(0, 100, 101)  # 0%,1%,...,100%

        # Interpolate the scores at these percentiles
        interpA = np.percentile(scoresA, percentiles)
        interpB = np.percentile(scoresB, percentiles)

        # Difference
        D = interpA - interpB

        # Find first intersection (negative -> positive)
        p_star, score_star = None, None
        for i in range(len(D)-1):
            if D[i] < 0 and D[i+1] > 0:
                p0, p1 = percentiles[i], percentiles[i+1]
                d0, d1 = D[i], D[i+1]
                p_star = p0 + (abs(d0)/(abs(d0)+abs(d1))) * (p1 - p0)
                score_star = interpA[i] + (interpA[i+1]-interpA[i]) * (p_star - p0)/(p1 - p0)
                break

        # Plot
        plt.figure(figsize=(8,5))
        plt.plot(percentiles, interpA, label=labelA, color='blue')
        plt.plot(percentiles, interpB, label=labelB, color='red')
        if p_star is not None:
            plt.axvline(p_star, color='green', linestyle='--', label=f'Intersection p={p_star:.2f}')
            plt.scatter(p_star, score_star, color='green', zorder=5)
        plt.xlabel('Percentile')
        plt.ylabel('Score')
        plt.title('Percentile-Score Curves with Intersection')
        plt.legend()
        plt.grid(True)
        plt.show()

        return p_star, score_star
                    
    def show_common_partners(self, char,output=True):
        honkai_stats =HonkaiStatistics_APOC(version=self.version, floor = self.floor, node=self.node,by_score=self.by_score, by_char=char,by_ed=self.by_ed)
        return honkai_stats.print_appearance_rate_by_char(output=output)
           
    def apirori(self, sort_by='Samples', ascending=False, output=True, antecedent_filter=None, consequent_filter=None):
        # Step 1: Get character appearance data
        t1 = time.time()
        data = HonkaiStatistics_APOC(version=self.version, floor=self.floor, node=self.node, by_score=self.by_score, by_ed=self.by_ed)
        data_char = data.print_appearance_rate_by_char(output=False)
        t2 = time.time()
        print(f"Time to get character appearance data: {t2 - t1:.4f} seconds")

        # Step 2: Map character name to appearance rate
        appearance_dict = dict(zip(data_char['Character'], data_char['Appearance Rate (%)']))
        appearance_prob = {k: v / 100 for k, v in appearance_dict.items()}  # Convert to probabilities

        # Step 3: Aggregate association data
        combined = pd.DataFrame()
        loop_times = []
        t3 = time.time()

        for x in data_char['Character']:
            loop_start = time.time()

            n = self.show_common_partners(x, output=False)
            n = n[n['Character'] != x]
            n = n.rename(columns={'Appearance Rate (%)': 'Confidence'})
            n['Antecedent'] = x
            n['Consequent'] = n['Character'].astype(str)
            n.drop(columns=['Character'], inplace=True)
            combined = pd.concat([combined, n], ignore_index=True)

            loop_elapsed = time.time() - loop_start
            loop_times.append((x, loop_elapsed))

        t4 = time.time()
        print(f"Total loop time: {t4 - t3:.4f} seconds")

        # Show top 5 slowest iterations
        slowest = sorted(loop_times, key=lambda x: x[1], reverse=True)[:5]
        print("Top 5 slowest characters in the loop:")
        for char, duration in slowest:
            print(f"  {char}: {duration:.4f} seconds")

        # Step 4: Reorder columns to have Antecedent and Consequent near the start
        t5 = time.time()
        cols = combined.columns.tolist()
        cols.insert(1, cols.pop(cols.index('Antecedent')))
        cols.insert(2, cols.pop(cols.index('Consequent')))
        combined = combined[cols]

        # Step 5: Calculate metrics
        combined['Appearance Rate (%)'] = round((combined['Samples'] / self.total_samples) * 100, 2)
        combined['Appearance Probability'] = combined['Samples'] / self.total_samples

        combined['Lift'] = combined.apply(
            lambda row: round(row['Confidence'] / appearance_dict.get(row['Consequent'], 1e-9), 4),
            axis=1
        )

        combined['Leverage'] = combined.apply(
            lambda row: round(
                (row['Appearance Probability'] -
                 (appearance_prob.get(row['Antecedent'], 0) * appearance_prob.get(row['Consequent'], 0))), 6),
            axis=1
        )

        combined['Conviction'] = combined.apply(
            lambda row: round(
                (1 - appearance_prob.get(row['Consequent'], 0)) /
                (1 - row['Confidence'] / 100 + 1e-9), 4),
            axis=1
        )

        # Move new columns right after 'Confidence'
        reordered_cols = combined.columns.tolist()
        key_cols = ['Appearance Rate (%)', 'Lift', 'Leverage', 'Conviction']
        for key in reversed(key_cols):
            reordered_cols.insert(reordered_cols.index('Confidence') + 1, reordered_cols.pop(reordered_cols.index(key)))
        combined = combined[reordered_cols]

        t6 = time.time()
        print(f"Time to reorder columns and compute metrics: {t6 - t5:.4f} seconds")

        # Step 6: Apply filtering
        if antecedent_filter:
            combined = combined[combined['Antecedent'] == antecedent_filter]
        if consequent_filter:
            combined = combined[combined['Consequent'] == consequent_filter]

        # Step 7: Sorting
        if sort_by not in combined.columns:
            print(f"Warning: '{sort_by}' not found. Defaulting to 'Confidence'")
            sort_by = 'Confidence'
        combined = combined.sort_values(by=sort_by, ascending=ascending).reset_index(drop=True)
        combined['Rank'] = range(1, len(combined) + 1)

        # Step 8: Output or return
        if output:
            print(combined.to_string(index=False))
            return
        return combined
                         
    def network_graph(self, sort_by="Weighted_Degree", weight_option="Samples", output=True, graph=False, clusters=True):
   

            # Get your apriori data
        d = self.apirori(output=False)
        df = pd.DataFrame({
            "Antecedent": d["Antecedent"],
            "Consequent": d["Consequent"],
            "Samples": pd.to_numeric(d["Samples"], errors='coerce'),
            "Appearance_Probability": pd.to_numeric(d["Appearance Probability"], errors='coerce'),
            "Lift": pd.to_numeric(d["Lift"], errors='coerce')
        })

        # Fill any NaNs with 0 to avoid comparison errors
        df = df.fillna(0)

        # ---- CREATE GRAPH ----
        G = nx.Graph()
        for idx, row in df.iterrows():
            a = row["Antecedent"]
            b = row["Consequent"]
            w = float(row[weight_option])  # ensure numeric
            G.add_edge(a, b, weight=w)

        # ---- CENTRALITY MEASURES ----
        weighted_degree = dict(G.degree(weight="weight"))
        degree = dict(G.degree())
        eigen_centrality = nx.eigenvector_centrality(G, weight="weight", max_iter=1000)

        # ---- CREATE DATAFRAME ----
        centrality_df = pd.DataFrame({
            "Character": list(G.nodes()),
            "Weighted_Degree": [weighted_degree[n] for n in G.nodes()],
            "Degree": [degree[n] for n in G.nodes()],
            "Eigenvector": [eigen_centrality[n] for n in G.nodes()]
        })

        # ---- SORT RESULTS ----
        if sort_by not in centrality_df.columns:
            sort_by = "Weighted_Degree"
        centrality_df = centrality_df.sort_values(by=sort_by, ascending=False).reset_index(drop=True)

        # ---- GRAPH ----
        if graph:
            plt.figure(figsize=(90, 50))
            pos = nx.spring_layout(G, k=2.0, iterations=500)

            nx.draw_networkx_nodes(G, pos, node_color='skyblue', node_size=150)
            edges = G.edges(data=True)
            max_weight = max([d['weight'] for (_, _, d) in edges])
            nx.draw_networkx_edges(
                G, pos,
                width=[d['weight']/max_weight * 5 for (_, _, d) in edges],
                alpha=0.6,
                edge_color='gray'
            )
            nx.draw_networkx_labels(G, pos, font_size=10, font_weight='bold')
            plt.title("Character Network Graph", fontsize=16)
            plt.axis('off')
            plt.show()

        # ---- OUTPUT ----
        if output:
            print(centrality_df.to_string(index=False))
            return

        return centrality_df

