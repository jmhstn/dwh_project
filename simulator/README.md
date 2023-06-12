# Simulator

## Actors

Each actor has:

* basic actions (like listen to a song/like a song/create a user/etc.)
* behaviours - logically grouped sequences of basic actions (with controlled intervals between them and probabilistic branching)
* schedulers - when and how often start a certain behaviour

### User Controller

One global actor.

* Has a pool of users and controls their lifetimes
    * registers new users
    * has access to the credentials table
    * spawns existing user actors (from time to time)
        * only those created by the sim, from its credentials table

### User

Multiple actors spawned by the User Controller.

* Models one user session from sign_in to sign_out/session timeout.
    * Listens to liked songs/artists/playlists
    * Looks for new songs (simple text queries? simple recommendations by liked songs?)
    * Likes new songs/artists/playlists
    * Signs out when it's done (or kills itself/is killed by the controller if idle for too long)

### Artist Controller

Spawns new artists and controls artist behaviours.

### Artist

* Periodically spawns new collections of songs




# simulator workflow:
# - spawn existing users (as agents)
#   - some random selection at a time (with a random delay?)
#   - each user has its behaviour and shuts down when it wants
# - register new users
#   - use some name generator library
# - spawn existing artists
#   - I dunno if we need them as separate type of users (for now no)
#   - artists spawn new releases
# - create new artists

# behaviour modelling:
# - users tend to have preferences: favorite artists and genres (so far genres are random)
# - users tend to choose liked collection/song much more likely, especially soon after the like, then they look for something new
# - users tend to choose new artists of the same genre more likely
# - duration for one session is normally distributed
# - number of songs listened in one session is normally distributed

# - number of releases and songs is more likely normally distributed (there could be parameters for different genres)
