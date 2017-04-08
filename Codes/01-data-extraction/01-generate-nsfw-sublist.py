
##########################################################################
## To generate a comma delimited list of nsfw sub-reddits from the file ##
## for use in the SQL Query 											##
##########################################################################

## Enter current location of the script and the `nsfwsub` file
cur_loc = ""

sublist = []

with open(cur_loc + 'nsfwsub') as f:
    for line in f:
        for word in line.split(" "):
            sublist.append(word)
            

sub = [ '"' + subr.strip().replace("/","")[1:] + '"' for subr in sublist if '/r/' in subr]

print ",".join(sub)