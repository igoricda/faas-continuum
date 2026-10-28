package config

// stripJSONC removes comments and trailing commas so that the result decodes as
// standard JSON.
func stripJSONC(src []byte) []byte {
	stripped := stripComments(src)
	return stripTrailingCommas(stripped)
}

// stripComments removes // and /* */ comments while leaving string literals
// untouched.
func stripComments(src []byte) []byte {
	out := make([]byte, 0, len(src))
	inString := false
	escaped := false
	inLineComment := false
	inBlockComment := false

	for i := 0; i < len(src); i++ {
		c := src[i]

		if inLineComment {
			if c == '\n' {
				inLineComment = false
				out = append(out, c)
			}
			continue
		}
		if inBlockComment {
			if c == '\n' {
				out = append(out, c)
			}
			if c == '*' && i+1 < len(src) && src[i+1] == '/' {
				inBlockComment = false
				i++
			}
			continue
		}

		if inString {
			out = append(out, c)
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == '"' {
				inString = false
			}
			continue
		}

		if c == '"' {
			inString = true
			out = append(out, c)
			continue
		}
		if c == '/' && i+1 < len(src) {
			next := src[i+1]
			if next == '/' {
				inLineComment = true
				i++
				continue
			}
			if next == '*' {
				inBlockComment = true
				i++
				continue
			}
		}
		out = append(out, c)
	}
	return out
}

// stripTrailingCommas removes commas followed only by whitespace and a closing
// brace or bracket, leaving string literals untouched.
func stripTrailingCommas(src []byte) []byte {
	out := make([]byte, 0, len(src))
	inString := false
	escaped := false

	for i := 0; i < len(src); i++ {
		c := src[i]

		if inString {
			out = append(out, c)
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == '"' {
				inString = false
			}
			continue
		}

		if c == '"' {
			inString = true
			out = append(out, c)
			continue
		}

		if c == ',' {
			j := i + 1
			for j < len(src) && (src[j] == ' ' || src[j] == '\t' || src[j] == '\n' || src[j] == '\r') {
				j++
			}
			if j < len(src) && (src[j] == '}' || src[j] == ']') {
				continue
			}
		}
		out = append(out, c)
	}
	return out
}
