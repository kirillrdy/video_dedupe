package main

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

type Database map[string][]float64

func crash(err error) {
	if err != nil {
		log.Panic(err)
	}
}

func dot(vector1, vector2 []float64) float64 {
	var sum float64

	//TODO maybe try shortest vector first
	if len(vector1) != len(vector2) {
		return 0
	}

	//TODO 200 is a very strange number
	if len(vector1) < 200 {
		return 0
	}

	for index := range vector1 {
		sum += vector1[index] * vector2[index]
	}
	return sum
}

func magnitude(vector []float64) float64 {
	return math.Sqrt(dot(vector, vector))
}

func cosine(vector1, vector2 []float64) float64 {
	return dot(vector1, vector2) / (magnitude(vector1) * magnitude(vector2))
}

func Fingerprint(filename string) ([]float64, error) {
	//TODO own dir
	tempFilename := fmt.Sprintf("/tmp/%d.mp3", rand.Int())
	err := exec.Command("ffmpeg", "-y", "-i", filename, tempFilename).Run()
	if err != nil {
		return []float64{}, errors.Wrap(err, fmt.Sprintf("ffmpeg failed for %s", filename))
	}

	outputBytes, err := exec.Command("fpcalc", "-length", "240", "-raw", tempFilename).Output()
	if err != nil {
		return []float64{}, errors.Wrap(err, "fpcalc failed")
	}

	regularExpressionForFingerPrint := regexp.MustCompile("FINGERPRINT=(.*)\n")
	fingerprint := regularExpressionForFingerPrint.FindStringSubmatch(string(outputBytes))[1]

	var numbers []float64
	for _, numberString := range strings.Split(fingerprint, ",") {
		number, err := strconv.Atoi(numberString)
		crash(err)
		numbers = append(numbers, float64(number))
	}

	return numbers, nil
}

func saveDb(db Database) {
	data, err := json.Marshal(&db)
	crash(err)
	err = ioutil.WriteFile("fingers.db", data, os.ModePerm)
	crash(err)
}

func loadHash() Database {
	var db Database
	data, err := ioutil.ReadFile("fingers.db")
	crash(err)

	err = json.Unmarshal(data, &db)
	crash(err)
	return db
}

func main2() {
	files, err := filepath.Glob("/storage/videos/*.mp4")
	crash(err)
	db := loadHash()
	for firstIndex := 0; firstIndex < len(files); firstIndex++ {
		firstFile := files[firstIndex]
		if _, ok := db[firstFile]; ok == false {
			continue
		}
		var dupes []string
		for secondIndex := firstIndex + 1; secondIndex < len(files); secondIndex++ {
			secondFile := files[secondIndex]
			if _, ok := db[secondFile]; ok == false {
				continue
			}

			f1 := db[firstFile]
			f2 := db[secondFile]
			diff := cosine(f1, f2)
			if diff > 0.95 {
				dupes = append(dupes, secondFile)
				fmt.Printf("%s,%s,%f\n", firstFile, secondFile, diff)
			}
		}
		if len(dupes) > 0 {
			dupeDirName := fmt.Sprintf("/storage/dupes/%d", firstIndex)
			err := os.MkdirAll(dupeDirName, os.ModePerm)
			crash(err)
			err = os.Symlink(firstFile, dupeDirName+"/"+"original.mp4")
			crash(err)
			for i, duplicate := range dupes {
				err = os.Symlink(duplicate, dupeDirName+"/"+fmt.Sprintf("dupe-%d.mp4", i))
				crash(err)
			}
		}

	}
}

func filesReader() {
	allFiles, err := filepath.Glob("/storage/videos/*.mp4")
	crash(err)
	for i, file := range allFiles {
		filesQueue <- file
		log.Print(i)
	}
	close(filesQueue)
}

func figerprintGenerator(index int, waitGroup *sync.WaitGroup) {
	if index != 0 {
		defer waitGroup.Done()
	}

	for file := range filesQueue {
		finger, err := Fingerprint(file)
		if err == nil {
			results <- Result{file, finger}
		}
	}
	if index == 0 {
		waitGroup.Done()
		waitGroup.Wait()
		close(results)
	}

}

func ResultsReciever() {
	db := loadHash()
	for result := range results {
		db[result.Filename] = result.Finger
	}
	saveDb(db)
}

type Result struct {
	Filename string
	Finger   []float64
}

var filesQueue = make(chan string)
var results = make(chan Result)

func main() {
	var waitGroup sync.WaitGroup
	go filesReader()

	for i := 0; i < 12; i++ {
		waitGroup.Add(1)
		go figerprintGenerator(i, &waitGroup)
	}

	ResultsReciever()
}
