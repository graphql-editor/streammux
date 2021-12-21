// Package streammux implements a very simple muxer over a pair
// of streams. It's meant to be used for very specific situations
// where user can guarantee a stable streams. It's main purpose for
// writing was creation of a low cost muxer for inter-process communication
// over stdio.
package streammux
