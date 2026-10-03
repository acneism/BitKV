package server

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

func TestGeo(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	palermo := strs("13.36138933897018433", "38.11555639549629859")
	catania := strs("15.08726745843887329", "37.50266842333162032")
	c.expect(int64(2), "GEOADD", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
	c.expect(status("zset"), "TYPE", "Sicily")
	c.expect("166274.1516", "GEODIST", "Sicily", "Palermo", "Catania")
	c.expect("166.2742", "GEODIST", "Sicily", "Palermo", "Catania", "km")
	c.expect("103.3182", "GEODIST", "Sicily", "Palermo", "Catania", "mi")
	c.expect(nil, "GEODIST", "Sicily", "Foo", "Bar")
	c.expect([]any{palermo, catania, nil}, "GEOPOS", "Sicily", "Palermo", "Catania", "NonExisting")
	c.expect(strs("sqc8b49rny0", "sqdtr74hyu0"), "GEOHASH", "Sicily", "Palermo", "Catania")
	c.expect(strs("Catania"), "GEORADIUS", "Sicily", "15", "37", "100", "km")
	c.expect(strs("Palermo", "Catania"), "GEORADIUS", "Sicily", "15", "37", "200", "km")
	c.expect([]any{strs("Palermo", "190.4424"), strs("Catania", "56.4413")}, "GEORADIUS", "Sicily", "15", "37", "200", "km", "WITHDIST")
	c.expect([]any{[]any{"Palermo", "190.4424", palermo}, []any{"Catania", "56.4413", catania}}, "GEORADIUS", "Sicily", "15", "37", "200", "km", "WITHDIST", "WITHCOORD")
	c.expect(int64(1), "GEOADD", "Sicily", "13.583333", "37.316667", "Agrigento")
	c.expect(strs("Agrigento", "Palermo"), "GEORADIUSBYMEMBER", "Sicily", "Agrigento", "100", "km")
	c.expect(int64(2), "GEORADIUS", "Sicily", "15", "37", "150", "km", "STORE", "near")
	c.expect(strs("Agrigento", "Catania"), "ZRANGE", "near", "0", "-1")

	c.expect(int64(2), "GEOADD", "Sic", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
	c.expect(int64(2), "GEOADD", "Sic", "12.758489", "38.788135", "edge1", "17.241510", "38.788135", "edge2")
	c.expect(strs("Catania", "Palermo"), "GEOSEARCH", "Sic", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC")
	c.expect([]any{
		[]any{"Catania", "56.4413", catania},
		[]any{"Palermo", "190.4424", palermo},
		[]any{"edge2", "279.7403", strs("17.24151045083999634", "38.78813451624225195")},
		[]any{"edge1", "279.7405", strs("12.7584877610206604", "38.78813451624225195")},
	}, "GEOSEARCH", "Sic", "FROMLONLAT", "15", "37", "BYBOX", "400", "400", "km", "ASC", "WITHCOORD", "WITHDIST")
	c.expect(int64(3), "GEOSEARCHSTORE", "key1", "Sic", "FROMLONLAT", "15", "37", "BYBOX", "400", "400", "km", "ASC", "COUNT", "3")
	c.expect([]any{
		[]any{"Catania", "56.4413", int64(3479447370796909), catania},
		[]any{"Palermo", "190.4424", int64(3479099956230698), palermo},
		[]any{"edge2", "279.7403", int64(3481342659049484), strs("17.24151045083999634", "38.78813451624225195")},
	}, "GEOSEARCH", "key1", "FROMLONLAT", "15", "37", "BYBOX", "400", "400", "km", "ASC", "WITHCOORD", "WITHDIST", "WITHHASH")
	c.expect(int64(3), "GEOSEARCHSTORE", "key2", "Sic", "FROMLONLAT", "15", "37", "BYBOX", "400", "400", "km", "ASC", "COUNT", "3", "STOREDIST")
	stored, _ := c.do("ZRANGE", "key2", "0", "-1", "WITHSCORES").([]any)
	for i, want := range []float64{56.441257870158204, 190.44242984775784, 279.7403417843143} {
		var got float64
		fmt.Sscan(stored[2*i+1].(string), &got)
		if stored[2*i] != []string{"Catania", "Palermo", "edge2"}[i] || math.Abs(got-want) > 1e-9 {
			t.Fatalf("STOREDIST stored %v", stored)
		}
	}
	c.expect(strs("edge2", "Catania"), "GEOSEARCH", "Sic", "FROMMEMBER", "edge1", "BYRADIUS", "400", "km", "DESC", "COUNT", "2")
	c.expect(strs("edge1", "Palermo"), "GEOSEARCH", "Sic", "FROMMEMBER", "edge1", "BYRADIUS", "400", "km", "COUNT", "2")
	if got, _ := c.do("GEOSEARCH", "Sic", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "COUNT", "1", "ANY").([]any); len(got) != 1 {
		t.Fatalf("GEOSEARCH COUNT 1 ANY = %v", got)
	}
	c.expect(strs(), "GEORADIUS", "missing", "15", "37", "200", "km")
	c.expect(strs(), "GEOSEARCH", "missing", "FROMMEMBER", "x", "BYRADIUS", "1", "km")
	c.expect(int64(0), "GEORADIUS", "missing", "15", "37", "200", "km", "STOREDIST", "near")
	c.expect(int64(0), "EXISTS", "near")

	c.expect(errReply("ERR invalid longitude,latitude pair 13.000000,95.000000"), "GEOADD", "Sicily", "13", "95", "x")
	c.expect(errReply(errNotFloat), "GEOADD", "Sicily", "abc", "1", "x")
	c.expect(errReply(errSyntax), "GEOADD", "Sicily", "1", "2", "x", "3")
	c.expect(errReply(errSyntax), "GEOADD", "Sicily", "XX", "NX", "1", "2", "x")
	c.expect(errReply(errSyntax), "GEODIST", "Sicily", "Palermo", "Catania", "km", "x")
	c.expect(errReply("ERR unsupported unit provided. please use M, KM, FT, MI"), "GEODIST", "Sicily", "Palermo", "Catania", "yd")
	c.expect(errReply("ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH"), "GEOSEARCH", "Sic", "BYRADIUS", "10", "km", "ASC", "WITHDIST")
	c.expect(errReply("ERR exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH"), "GEOSEARCH", "Sic", "FROMLONLAT", "15", "37", "ASC", "WITHDIST")
	c.expect(errReply(errGeoMember), "GEOSEARCH", "Sic", "FROMMEMBER", "nobody", "BYRADIUS", "10", "km")
	c.expect(errReply("ERR the ANY argument requires COUNT argument"), "GEOSEARCH", "Sic", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ANY")
	c.expect(errReply("ERR COUNT must be > 0"), "GEOSEARCH", "Sic", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "COUNT", "0")
	c.expect(errReply("ERR STORE option in GEORADIUS is not compatible"), "GEORADIUS", "Sic", "15", "37", "200", "km", "STORE", "dst", "WITHDIST")
	c.expect(errReply("ERR GEOSEARCHSTORE is not compatible"), "GEOSEARCHSTORE", "dst", "Sic", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "WITHDIST")
	c.expect(errReply(errSyntax), "GEORADIUS_RO", "Sic", "15", "37", "200", "km", "STORE", "dst")
	c.expect(errReply("ERR radius cannot be negative"), "GEORADIUS", "Sic", "15", "37", "-1", "km")
	c.expect(errReply("ERR height or width cannot be negative"), "GEOSEARCH", "Sic", "FROMLONLAT", "15", "37", "BYBOX", "-1", "1", "km")
	c.expect(errReply("ERR need numeric radius"), "GEOSEARCH", "Sic", "FROMLONLAT", "15", "37", "BYRADIUS", "x", "km")
	c.expect(status("OK"), "SET", "str", "v")
	c.expect(errReply(errWrongType), "GEOADD", "str", "1", "2", "x")
	c.expect(errReply(errWrongType), "GEOPOS", "str", "x")
	c.expect(errReply(errWrongType), "GEOSEARCH", "str", "FROMLONLAT", "15", "37", "BYRADIUS", "1", "km")
}

func TestGeoSearchFindsEveryPoint(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	rng := rand.New(rand.NewPCG(11, 12))
	args := []string{"GEOADD", "points"}
	for i := range 1500 {
		lon, lat := -10+rng.Float64()*20, 60+rng.Float64()*25
		args = append(args, fmt.Sprint(lon), fmt.Sprint(lat), fmt.Sprint("p", i))
	}
	c.expect(int64(1500), args...)
	c.expect("skiplist", "OBJECT", "ENCODING", "points")
	z := map[string][2]float64{}
	for i := range 1500 {
		pos, _ := c.do("GEOPOS", "points", fmt.Sprint("p", i)).([]any)
		var lon, lat float64
		fmt.Sscan(pos[0].([]any)[0].(string), &lon)
		fmt.Sscan(pos[0].([]any)[1].(string), &lat)
		z[fmt.Sprint("p", i)] = [2]float64{lon, lat}
	}
	for round := range 40 {
		s := geoShape{lon: -10 + rng.Float64()*20, lat: 60 + rng.Float64()*25, conversion: 1000}
		query := []string{"GEOSEARCH", "points", "FROMLONLAT", fmt.Sprint(s.lon), fmt.Sprint(s.lat)}
		if round%2 == 0 {
			s.radius = 1 + rng.Float64()*800
			query = append(query, "BYRADIUS", fmt.Sprint(s.radius), "km")
		} else {
			s.box, s.width, s.height = true, 1+rng.Float64()*1500, 1+rng.Float64()*1500
			query = append(query, "BYBOX", fmt.Sprint(s.width), fmt.Sprint(s.height), "km")
		}
		var want []string
		for member, p := range z {
			if _, ok := s.contains(p[0], p[1]); ok {
				want = append(want, member)
			}
		}
		got := members(t, c, query...)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("round %d %v: %d members, want %d", round, query, len(got), len(want))
		}
	}
}
