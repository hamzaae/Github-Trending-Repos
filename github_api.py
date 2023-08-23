import requests
from dotenv import load_dotenv
import os


def extractTrendingRepos():
    load_dotenv()
    # GitHub personal access token
    githubToken = os.getenv("GITHUB_TOKEN")


    # API endpoint URL for searching repositories
    url = "https://api.github.com/search/repositories"

    # Parameters for the search query (adjust as needed)
    createdDate = "2023-07-01"
    params = {
        "q": f"created:>{createdDate}",  # Trending repositories created after this date
        "sort": "stars",
        "order": "desc",
        "per_page": 10  # Number of repositories per page
    }

    # Headers for API request
    headers = {
    "Authorization": f"token {githubToken}",
    "Accept": "application/vnd.github.v3+json"
}


    # Fetch repositories using the GitHub API
    response = requests.get(url, headers=headers, params=params)
    data = response.json()

    # Process and calculate trendiness scores
    trending_repositories = []
    for repo in data.get("items", []):
        try:
            # Fetch more details about each repository
            repo_url = repo["url"]
            repo_response = requests.get(repo_url, headers=headers)
            repo_data = repo_response.json()
            print(repo_data)

            # Extract additional repository details
            name = repo_data["name"]
            owner = repo_data["owner"]["login"]
            stars = repo_data["stargazers_count"]
            forks = repo_data["forks"]
            commits = 10  # Placeholder value (tmp)
            issues = repo_data["open_issues"]
            description = repo_data["description"]
            language = repo_data["language"]

            # Calculate trendiness score using custom weights
            trendiness_score = (0.4 * stars) + (0.3 * forks) + (0.2 * commits) + (0.1 * issues)

            # Append repository details and trendiness score
            trending_repositories.append({
                "name": name,
                "owner": owner,
                "description": description,
                "language": language,
                "stars": stars,
                "forks": forks,
                "trendiness_score": trendiness_score
            })
        except Exception as ex:
            print(ex)
    # Print the list of trending repositories with more details

    # for repo in trending_repositories:
    #     print(f"Repository: {repo['owner']}/{repo['name']}")
    #     print(f"Description: {repo['description']}")
    #     print(f"Language: {repo['language']}")
    #     print(f"Stars: {repo['stars']}, Forks: {repo['forks']}, Trendiness Score: {repo['trendiness_score']:.2f}")
    #     print("------------------------------")

    return trending_repositories

# extractTrendingRepos()