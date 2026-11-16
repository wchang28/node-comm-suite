import {spawn} from 'child_process';
import { PassThrough } from 'stream';
import {text} from 'stream/consumers';

//const command = 'ls /etc';
//const command = 'ls /xxxyyyzzz';
//const command = 'npmm --version';
//const command = (process.platform === 'win32' ? "sleep infinity" : "sleep infinity");
const command = (process.platform === 'win32' ? "echo Hi > c:\\tmp\\hi.txt" : "echo Hi > /mnt/c/tmp/hi.txt");
const stdinString = "";
//const command = 'cat';
//const stdinString = "Your UTF-8 text here"; // You can change this to provide input to the child process if needed

const child = spawn(command, {shell: true, env: {}});
const pid = child.pid;
child.stdin.setDefaultEncoding('utf-8');
child.stdout.setEncoding('utf-8');
child.stderr.setEncoding('utf-8');

if (stdinString) {
    child.stdin.write(stdinString);
    child.stdin.end();
}

const stdoutStringStream = new PassThrough();
const stderrStringStream = new PassThrough();

child.stdout.pipe(stdoutStringStream);
child.stderr.pipe(stderrStringStream);

child.on('close', async (code) => {
    // !!! on Linux code can be null if the process was killed by a signal, or a number if it exited normally !!!
    // !!! on Windows code is always a number, even if the process was killed by a signal (in that case, code will be 1) !!!
    console.log(`Child process (pid=${pid}) exited with code ${code}`);
    if (code === null) {    // on linux, if the process was killed by a signal, code will be null and signal will be set to the name of the signal that killed the process
        code = 1;
    }
    const stdoutString = await text(stdoutStringStream);
    const stderrString = await text(stderrStringStream);
    console.log('---------------------------- STDOUT -----------------');
    console.log(stdoutString);
    console.log('---------------------------- STDERR -----------------');
    console.log(stderrString);
    console.log('-----------------------------------------------------');
});

console.log(`Child process PID is ${pid}`);