import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter
import os
from multiprocessing import Pool, cpu_count

from Appearance_rate_Apocalytic_Shadow import HonkaiStatistics_APOC
from Appearance_rate_Pure_fiction import HonkaiStatistics_Pure
from Appearance_rate import HonkaiStatistics


class HonkaiAnalyzer:
    """
    Analyzes and visualizes character performance across different Honkai Star Rail 
    end-game modes (MOC, PURE, APOC).

    The 'character' argument determines the scope of the analysis:
    - string (e.g., 'Anaxa'): Aggregated stats for the character across all team pairings.
    - tuple (e.g., ('Anaxa',)): Specific stats for the character as a solo primary dealer.
    - tuple (e.g., ('The Herta', 'Anaxa')): Specific stats for that exact archetype/pairing.
    """

    def __init__(self, character: str, version_list: list, mode: str, floor: int = None, by_ed: int = 0):
        """
        Initialize the analyzer.

        Args:
            character: The name (str) or archetype (tuple) to analyze.
            version_list: List of game version strings to process.
            mode: The game mode ('APOC', 'PURE', or 'MOC').
            floor: The specific floor/stage to analyze. Defaults to 4 for APOC/PURE, 12 for MOC.
            by_ed: Filter by Eidolon level (default 0).
        """
        self.character = character
        self.versions = version_list
        self.mode = mode
        self.by_ed = by_ed
        self.floor = floor
        self.scores = []
        self.avg_list = []
        self.app_list = []
        self.max_list = []
        self.df = None

        self._select_stat_class()

    def _select_stat_class(self):
        """Sets internal configuration based on the chosen game mode."""
        if self.mode == "APOC":
            self.stat_class = HonkaiStatistics_APOC
            self.key = "Scores"
            self.title = "Histogram of APOC Scores"
            if self.floor is None:
                self.floor = 4
        elif self.mode == "PURE":
            self.stat_class = HonkaiStatistics_Pure
            self.key = "Points"
            self.title = "Histogram of Pure Fiction Points"
            if self.floor is None:
                self.floor = 4
        elif self.mode == "MOC":
            self.stat_class = HonkaiStatistics
            self.key = "Avg Cycles"
            self.title = "Histogram of MOC Cycles"
            if self.floor is None:
                self.floor = 12
        else:
            raise ValueError("Invalid mode. Use 'APOC', 'PURE', or 'MOC'.")

    def _process_version(self, version):
        """
        Internal worker to process a single version.
        Handles data extraction for both single characters (str) and archetypes (tuple).
        """
        try:
            stats = self.stat_class(version=version, floor=self.floor, by_ed=self.by_ed)
            
            if type(self.character) == tuple:
                if len(self.character) > 1:
                    # Sort tuple by internal index to match archetype keys
                    input_val = tuple(sorted(self.character, key=lambda d: stats.chars[d]['Index']))
                else:
                    input_val = self.character
                
                dic = stats.archetypes[input_val]
                df = stats.print_archetypes(output=False)
                
                if len(self.character) > 1:
                    df = df[df["Archetype"] == f"{input_val}"] 
                else:
                    # Specific formatting for single-item tuples in the stats dataframe
                    df = df[df["Archetype"] == f"('{input_val[0]}')"]
            
            elif type(self.character) == str: 
                dic = stats.chars[self.character]
                df = stats.print_appearance_rate_by_char(output=False)
                df = df[df["Character"] == self.character]   
                
            result = {
                'scores': dic[self.key],
                'app_rate': df["Appearance Rate (%)"].iloc[0] if not df.empty else 0,
                'avg': df.iloc[0, 8] if not df.empty else 0,
                'max': df.iloc[0, 10] if (not df.empty and self.mode != "MOC") else (df.iloc[0, 4] if not df.empty else 0)
            }
            
            return result
        except Exception as e:
            print(f"Skipping version {version} due to error: {e}")
            return None

    def analyze(self):
        """
        Executes the analysis across all specified versions in parallel 
        using the available CPU cores.
        """
        num_workers = min(cpu_count() - 1, len(self.versions))

        with Pool(num_workers) as pool:
            results = pool.map(self._process_version, self.versions)

        valid_results = [r for r in results if r is not None]
        valid_versions = [v for r, v in zip(results, self.versions) if r is not None]

        for result in valid_results:
            self.scores += result['scores']
            self.app_list.append(result['app_rate'])
            self.avg_list.append(result['avg'])
            self.max_list.append(result['max'])

        self.df = pd.DataFrame({self.key: self.scores})
        self.versions = valid_versions

    def show_summary(self):
        """Prints a statistical summary and a per-version breakdown to the console."""
        print(f"Summary for Character: {self.character}")
        print(self.df.describe())

        counter = Counter(self.df[self.key].tolist())
        sorted_counts = sorted(counter.items(), key=lambda x: x[0], reverse=True)
        print(sorted_counts)

        summary_df = pd.DataFrame({
            "Character": [self.character] * len(self.versions),
            "Version": self.versions,
            f"Average {self.key}": self.avg_list,
            "Appearance Rate": self.app_list,
            f"{'Max' if self.mode != 'MOC' else 'Min'} {self.key}": self.max_list
        })

        print(summary_df.to_string(index=False))

    def show_histogram(self):
        """Generates and displays a histogram of the collected scores/cycles."""
        plt.figure()
        self.df[self.key].hist()
        plt.title(f"{self.title} for {self.character}")
        plt.xlabel(self.key)
        plt.ylabel("Frequency")
        plt.show()
    
    def _single_summary(self):
        """Returns a transposed description of the scores."""
        desc = self.df.describe().T
        desc = desc.rename(index={'Scores': f"{self.character}"})
        return desc


# Example usage:
if __name__ == "__main__":
    analyzer_apoc = HonkaiAnalyzer(
        character=('Acheron',),
        version_list=["2.3.1", "2.4.1", "2.5.1", "2.6.1", "2.7.1", "3.0.3", "3.1.3", "3.2.3","3.3.3","3.4.3","3.5.3","3.6.3.2","3.7.3","3.8.3","4.0.2"],
        mode="APOC",
        floor=4,         # Customizable
        by_ed=0 # Customizable
    )
    analyzer_apoc.analyze()
    analyzer_apoc.show_summary()
    analyzer_apoc.show_histogram()

    analyzer_pure = HonkaiAnalyzer(
        character=('Acheron',),
        version_list=["2.3.2", "2.4.2", "2.5.2", "2.6.2", "2.7.2", "3.1.1", "3.2.1","3.3.1","3.4.1","3.5.1","3.6.1","3.7.1","3.8.1","3.8.4","4.0.2"],
        mode="PURE",
        floor=4,
        by_ed=0
    )
    analyzer_pure.analyze()
    analyzer_pure.show_summary()
    analyzer_pure.show_histogram()

    analyzer_moc = HonkaiAnalyzer(
        character=('Acheron',),
        version_list=["2.3.3", "2.4.3", "2.5.3", "2.6.3", "2.7.3", "3.1.2", "3.2.2","3.3.2","3.4.2","3.5.2","3.6.2","3.7.2","3.8.2","4.0.1","4.0.2"],
        mode="MOC",
        floor=12,
        by_ed=0
    )
    analyzer_moc.analyze()
    analyzer_moc.show_summary()
    analyzer_moc.show_histogram()