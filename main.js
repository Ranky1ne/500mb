import {
  createReadStream,
  createWriteStream,
  writeFileSync,
  unlink,
  mkdir,
  rmdir,
} from "fs";
import { createInterface } from "readline";

// Defines variables: Input file, file for output of sorted values, name of the directory for split files, size of split files, variable for denoting the index of split files, create a folder for temporary files
const inputFile = "input.txt";
const outputFileName = "output.txt";
const chunksDir = "chunks";
const chunkSize = 80 * 1024 * 1024; // 80 MB
let chunkNumber = 1;
mkdir(chunksDir, (err) => {
  if (err) {
  } else {
    console.log("Folder chunks created");
  }
});
// Function for splitting a large file
// Creates a read stream for the input file, an array for processing lines,
// a variable to determine the size of the array in bytes,
// opens a read stream for the input file line by line using readline
async function splitFile() {
  console.log("Start split");

  const inputReadStream = createReadStream(inputFile, { encoding: "utf-8" });
  const chunkLines = [];
  let currentSize = 0;
  const rl = createInterface({
    input: inputReadStream,
    crlfDelay: Infinity,
  });

  // Function for creating and sorting a temporary file to store lines,
  // creates a file in the "chunks" directory to store lines, opens a write stream to the created file
  // Sorts the array in alphabetical order, writes lines from the array to the temporary file, waits for the completion of writing to the file using the end method of the write stream
  // Increments the file index by 1, updates the file size variable, and clears the array
  const createChunk = async () => {
    const chunkFile = `${chunksDir}/chunk_${chunkNumber}.txt`;
    const chunkWriteStream = createWriteStream(chunkFile, {
      encoding: "utf-8",
    });

    chunkLines.sort((a, b) => a.localeCompare(b));
    chunkLines.forEach((line) => chunkWriteStream.write(`${line}\n`));
    await new Promise((resolve) => chunkWriteStream.end(resolve));

    chunkNumber += 1;
    currentSize = 0;
    chunkLines.length = 0;
  };

  // Loop for reading lines from the input file, adding a line to the array, and increasing a variable by the size of this line in bytes,
  // when the set file size is exceeded, the function is called to create a new file, upon completion of file processing, write all remaining values to a new file
  // Close the read stream
  for await (const line of rl) {
    chunkLines.push(line);
    currentSize += Buffer.from(line).length;

    if (currentSize >= chunkSize) {
      await createChunk();
    }
  }

  if (chunkLines.length > 0) {
    await createChunk();
  }

  rl.close();
}

// Function for reading and merging temporary files
async function readAndMerge(num) {
  console.log("Start merge");
  // The function creates read streams line by line for each temporary file,
  // creates an iterator for controlled processing of each line separately, and adds it to the array
  const createRl = (chunkNumber) => {
    const arr = [];
    for (let i = 1; i < chunkNumber; i++) {
      const inputReadStream = createReadStream(`${chunksDir}/chunk_${i}.txt`, {
        encoding: "utf-8",
      });
      const rl = createInterface({
        input: inputReadStream,
        crlfDelay: Infinity,
      });
      const iterator = rl[Symbol.asyncIterator]();
      arr.push(iterator);
    }
    return arr;
  };

  // Invoke a function to create a specific number of read streams, define an array to store the values of each line from temporary files, and set their index
  const iterator = createRl(num);
  const lineArr = [];
  let ind = 0;

  // Loop for adding pairs {line value, index} to the array, sorting these values, creating a file to record the result (or deleting the contents)
  // Create a write stream to the file
  for await (const line of iterator) {
    lineArr.push({ value: (await line.next()).value, index: ind });
    ind++;
  }
  lineArr.sort((a, b) => a.value.localeCompare(b.value));
  writeFileSync(`./${outputFileName}`, "", "utf-8");
  const outputWriteStream = createWriteStream(`./${outputFileName}`, {
    encoding: "utf-8",
  });

  // Function for sorting lines and writing to the output file
  // Invoke a loop to process the array of lines until there are no more values in the array
  // Write the first line to the file, read and write the next line value from the temporary file to the array, delete the object if values are undefined
  // Sort the values in the array with the new value in alphabetical order
  // After completion, close the write stream
  async function sortAndMearge(arr) {
    while (arr.length !== 0) {
      outputWriteStream.write(`${arr[0].value}\n`);
      arr[0].value = (await iterator[arr[0].index].next()).value;
      if (arr[0].value === undefined) {
        arr.shift();
      }
      arr.sort((a, b) => a.value.localeCompare(b.value));
    }
    outputWriteStream.end();
  }
  await sortAndMearge(lineArr);
}

async function externalSort() {
  console.time("Step 1");
  // Step 1: Split the large file into smaller sorted chunks
  await splitFile();
  console.timeEnd("Step 1");
  console.time("Step 2");
  // Step 2: Merge the sorted chunks to produce the final sorted file
  await readAndMerge(chunkNumber);
  for (let i = chunkNumber - 1; i > 0; i--) {
    unlink(`./chunks/chunk_${i}.txt`, (err) => {
      if (err) throw err;
    });
  }
  console.log("Chunks deleted");
  console.timeEnd("Step 2");
}

externalSort();
