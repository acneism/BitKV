package server

func matchGlob(pattern, s string) bool {
	var exhausted bool
	return globMatch(pattern, s, &exhausted)
}

func globMatch(pattern, s string, exhausted *bool) bool {
	for len(pattern) > 0 {
		switch pattern[0] {
		case '*':
			for len(pattern) > 1 && pattern[1] == '*' {
				pattern = pattern[1:]
			}
			if len(pattern) == 1 {
				return true
			}
			for ; len(s) > 0; s = s[1:] {
				if globMatch(pattern[1:], s, exhausted) {
					return true
				}
				if *exhausted {
					return false
				}
			}
			*exhausted = true
			return false
		case '?':
			if len(s) == 0 {
				return false
			}
		case '[':
			if len(s) == 0 {
				return false
			}
			rest, ok := matchClass(pattern[1:], s[0])
			if !ok {
				return false
			}
			pattern, s = rest, s[1:]
			continue
		case '\\':
			if len(pattern) > 1 {
				pattern = pattern[1:]
			}
			fallthrough
		default:
			if len(s) == 0 || pattern[0] != s[0] {
				return false
			}
		}
		pattern, s = pattern[1:], s[1:]
	}
	return len(s) == 0
}

func matchClass(p string, ch byte) (string, bool) {
	negate := len(p) > 0 && p[0] == '^'
	if negate {
		p = p[1:]
	}
	matched := false
	for len(p) > 0 && p[0] != ']' {
		switch {
		case p[0] == '\\' && len(p) >= 2:
			matched = matched || p[1] == ch
			p = p[2:]
		case len(p) >= 3 && p[1] == '-':
			lo, hi := p[0], p[2]
			if lo > hi {
				lo, hi = hi, lo
			}
			matched = matched || (ch >= lo && ch <= hi)
			p = p[3:]
		default:
			matched = matched || p[0] == ch
			p = p[1:]
		}
	}
	if len(p) > 0 {
		p = p[1:]
	}
	return p, matched != negate
}
