package git

type PullRequest struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	State       string `json:"state"`
	CreatedDate string `json:"created_date"`
	UpdatedDate string `json:"updated_date"`
	Author      string `json:"author"`
}

type RawRepositoryOperation struct {
	Values []struct {
		ID          string `json:"id"`
		UpdatedDate string `json:"updatedDate"`
	} `json:"values"`
	IsLastPage    bool   `json:"isLastPage"`
	NextPageStart string `json:"nextPageStart"`
}

type RawPullRequestDetail struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	State       string `json:"state"`
	CreatedDate string `json:"createdDate"`
	UpdatedDate string `json:"updatedDate"`
	Author      struct {
		User struct {
			DisplayName string `json:"displayName"`
		} `json:"user"`
	} `json:"author"`
}

type Links struct {
	Self string `json:"self"`
}

// Content represents a file in a Bitbucket repository
type Content struct {
	ID           string `json:"id"`
	Path         string `json:"path"`
	Content      string `json:"content"`
	Type         string `json:"type"`
	URL          string `json:"url"`
	LastModified string `json:"lastModified"`
	Xxh3         string `json:"xxh3"`
}
