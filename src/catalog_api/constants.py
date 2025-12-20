PROVIDERS = ["Netflix", "Prime", "Max", "Disney+", "AppleTV", "Paramount"]

PROVIDER_NEEDLES = {
    "Netflix": ("Netflix",),
    "Prime": ("Prime",),
    "Max": ("Max",),
    "Disney+": ("Disney+", "Disney"),
    "AppleTV": ("Apple TV", "AppleTV", "Apple TV+"),
    "Paramount": ("Paramount", "Paramount+"),
}

HOME_GENRES = {
    "Comedy": ("Comedy",),
    "Action": ("Action",),
    "Horror": ("Horror",),
    "Romance": ("Romance",),
    "SciFi": ("Science Fiction", "Sci-Fi & Fantasy", "Sci-Fi"),
    "Drama": ("Drama",),
    "Animation": ("Animation",),
}

BROWSE_TABS = {
    "popular": ("popular", None),
    "rating": ("rating", None),
    "recent": ("recent", None),
    "action": ("genre", "Action"),
    "adventure": ("genre", "Adventure"),
    "animation": ("genre", "Animation"),
    "comedy": ("genre", "Comedy"),
    "crime": ("genre", "Crime"),
    "documentary": ("genre", "Documentary"),
    "drama": ("genre", "Drama"),
    "family": ("genre", "Family"),
    "fantasy": ("genre", "Fantasy"),
    "history": ("genre", "History"),
    "horror": ("genre", "Horror"),
    "music": ("genre", "Music"),
    "mystery": ("genre", "Mystery"),
    "romance": ("genre", "Romance"),
    "science-fiction": ("genre", "Science Fiction"),
    "tv-movie": ("genre", "TV Movie"),
    "thriller": ("genre", "Thriller"),
    "war": ("genre", "War"),
    "western": ("genre", "Western"),
}
