package usage

// ImageDetail is what an image request asked for and what came back (tokenmaxx fork). The
// request log stores it as the image_detail JSON column; nil on every non-image record.
// Requested fields are as sent upstream and empty when the client left them out; returned
// fields describe the first image, ReturnedCount all of them.
type ImageDetail struct {
	Operation           string `json:"op"` // generate | edit
	RequestedSize       string `json:"requested_size,omitempty"`
	RequestedQuality    string `json:"requested_quality,omitempty"`
	RequestedBackground string `json:"requested_background,omitempty"`
	RequestedFormat     string `json:"requested_format,omitempty"`
	RequestedN          int64  `json:"requested_n,omitempty"`
	InputImages         int    `json:"input_images,omitempty"` // edit inputs
	Mask                bool   `json:"mask,omitempty"`
	ReturnedCount       int    `json:"returned_count"`
	ReturnedSize        string `json:"returned_size,omitempty"`
	ReturnedQuality     string `json:"returned_quality,omitempty"`
	ReturnedBackground  string `json:"returned_background,omitempty"`
	ReturnedFormat      string `json:"returned_format,omitempty"`
}

// Clone returns a copy, or nil.
func (d *ImageDetail) Clone() *ImageDetail {
	if d == nil {
		return nil
	}
	c := *d
	return &c
}
