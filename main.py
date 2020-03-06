from crawler import JockeyClub

if __name__ == "__main__":
    # instantiate class
    jc = JockeyClub()
    # crawl races and horse handicaps
    races = jc.crawl_races()
    handicaps = jc.crawl_handicaps()
    # export to csv
    races.to_csv("data/races.csv", index=False)
    handicaps.to_csv("data/handicaps.csv", index=False)
