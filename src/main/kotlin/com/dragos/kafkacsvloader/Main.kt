package com.dragos.kafkacsvloader

import com.dragos.kafkacsvloader.cli.LoadCommand
import com.github.ajalt.clikt.core.subcommands

fun main(args: Array<String>) {
    LoadCommand()
        .subcommands()
        .main(args)
}