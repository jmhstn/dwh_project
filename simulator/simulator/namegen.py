import random
from numbers import Number
from typing import Any, Optional


import inflect
from faker import Faker
from faker.providers import address, lorem
from faker.providers import BaseProvider, address, date_time, internet, misc


fake = Faker()
fake.add_provider(lorem)
fake.add_provider(address)
fake.add_provider(date_time)
fake.add_provider(internet)
fake.add_provider(misc)

_inflect = inflect.engine()


def choose_weighted(choices, with_none=False):
    xs = list(zip(*choices.items()))
    sum_w = sum(xs[1])
    if with_none and sum_w < 1:
        xs = (xs[0] + (None,), xs[1] + (1.0 - sum_w,))
    return random.choices(*xs, k=1)[0]


prefix_words = {"My": 7, "Your": 4, "Our": 2, "His": 2, "Her": 2, "Some": 1}


def _add_prefix(word: str) -> str:
    prefix = choose_weighted(prefix_words)
    return f"{prefix} {word}"


def _gen_word(parts_of_speech={"noun": 0.8, "adjective": 0.2}, plural_p=0.95) -> str:
    pos = choose_weighted(parts_of_speech)
    word = fake.word(part_of_speech=pos).capitalize()
    name = _inflect.plural(word) if random.random() < plural_p else word
    return name


def _articalize(word: str, p=0.8) -> str:
    return f"The {word}" if random.random() < p else word


def _gen_person_name() -> str:
    # artist name
    if random.random() < 0.5:
        return fake.name_female()
    else:
        return fake.name_male()


def _gen_band_name(articalize_p=0.7, complex_name_p=0.2) -> str:
    one_word_p = 0.5
    if random.random() < one_word_p:
        # one word name
        name = _articalize(_gen_word(), p=articalize_p)
    else:
        # two word name
        first = _gen_word(
            {"noun": 0.2, "adjective": 0.4, "verb": 0.2, "adverb": 0.2}, plural_p=0
        )
        second = _gen_word()
        name = _articalize(f"{first} {second}", p=articalize_p)
    if random.random() < complex_name_p:
        word = _gen_band_name(articalize_p=0.9, complex_name_p=0)
        link = "of" if random.random() > 0.75 else "and"
        name = f"{name} {link} {word}"
    with_prefix_p = 0.3
    if random.random() < with_prefix_p and not name.startswith("The"):
        name = _add_prefix(name)
    return name


def _change_case(word: str) -> str:
    if random.random() < 0.9:
        word = word.lower()
        if random.random() < 0.1:
            word = f"{word}."
        return word
    else:
        return word.upper()


def generate_artist_name() -> str:
    band_p = 0.8
    if random.random() < band_p:
        with_author_p = 0.01
        if random.random() < with_author_p:
            name = f"{_gen_person_name()} and {_gen_band_name(articalize_p=1.0)}"
        else:
            band = _gen_band_name()
            name = band if random.random() < 0.9 else _change_case(band)
        return name
    else:
        return _gen_person_name()


def generate_song_name() -> str:
    r = random.random()
    if r < 0.6:
        # one random word
        name = _articalize(_gen_word(plural_p=0.3), 0.02)
    elif r < 0.75:
        # analogous to band names
        name = _gen_band_name(articalize_p=0.1, complex_name_p=0.01)
    elif r < 0.95:
        # random words
        name = " ".join(
            [
                _gen_word(
                    {"noun": 0.5, "adjective": 0.3, "verb": 0.2, "adverb": 0.2},
                    plural_p=0.5,
                )
                for _ in range(random.randint(2, 3))
            ]
        )
    else:
        # adj/verb/adverb + word
        adj = _gen_word({"adjective": 1, "verb": 0.5, "adverb": 0.2}, plural_p=0)
        word = _gen_word(plural_p=0.5)
        name = _articalize(f"{adj} {word}", p=0.02)
    return name


base_genres = {
    "Pop": 10,
    "Rock": 10,
    "Rap": 5,
    "Jazz": 5,
    "Metal": 5,
    "Techno": 3,
    "Ambient": 3,
    "House": 3,
    "Trance": 3,
    "Drone": 1,
    "Funk": 1,
    "Blues": 1,
    "Soul": 1,
    "Shoegaze": 1,
    "Garage": 1,
    "Noise": 1,
    "Country": 1,
}

base_genre_suffixes = {
    "core": 3,
    "step": 1,
    "tronica": 2,
    "tune": 1,
    "beat": 1,
    "wave": 5,
}

genre_prefixes = {
    "Post-": 10,
    "Avant-": 5,
    "Proto-": 1,
    "Minimal ": 2,
    "Symphonic ": 1,
    "Meta-": 1,
    "Lofi ": 5,
    "Indie ": 10,
    "Alternative ": 7,
    "Bedroom ": 2,
    "Instrumental ": 2,
    "Progressive ": 3,
    "Industrial ": 2,
    "Synth ": 1,
    "Acoustic ": 1,
    "New ": 1,
}


def _generate_base_genre() -> str:
    r = random.random()
    if r < 0.5:
        base = choose_weighted(base_genres)
    else:
        base = _gen_word(plural_p=0)
    if random.random() < 0.3:
        suffix = choose_weighted(base_genre_suffixes)
        base = f"{base}{suffix}"
    return base


def generate_genre_name(base: Optional[str] = None) -> str:
    if base is None:
        base = _generate_base_genre()
    p = random.random()
    if p < 0.2:
        # add country
        country = fake.country()
        genre = f"{country} {base}"
    elif p < 0.8:
        # add prefix
        prefix = choose_weighted(genre_prefixes)
        genre = f"{prefix}{base}"
    else:
        # add random word
        word = _gen_word(plural_p=0)
        genre = f"{word} {base}"
    if random.random() < 0.1:
        second = generate_genre_name()
        base = f"{base} {second}"
    return genre


class MusicProvider(BaseProvider):
    def artist(self) -> str:
        return generate_artist_name()

    def song(self) -> str:
        return generate_song_name()

    def collection(self) -> str:
        # same generator as for songs
        return generate_song_name()

    def genre(self, base: Optional[str] = None) -> str:
        return generate_genre_name(base)


fake.add_provider(MusicProvider)
